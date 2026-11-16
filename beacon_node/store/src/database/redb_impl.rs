use crate::{ColumnIter, ColumnKeyIter, Key, metrics};
use crate::{DBColumn, Error, KeyValueStoreOp};
use parking_lot::{Mutex, RwLock};
use redb::{ReadableDatabase, TableDefinition};
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;
use strum::IntoEnumIterator;
use tracing::{error, info, warn};

use super::interface::WriteOptions;

pub const DB_FILE_NAME: &str = "database.redb";

type BytesTable<'txn> = redb::Table<'txn, &'static [u8], &'static [u8]>;

/// How often `sync` should be called to make commits durable.
///
/// A crash loses at most about this much of the most recent writes. Explicit syncs, such as
/// `put_bytes_sync`, are durable when they return.
pub const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

// Lock ordering, to avoid deadlocks:
//
// 1. `FLUSH_LOCK`.
// 2. `OPEN_DATABASES`, or a `Shared::db` lock.
// 3. A redb write transaction.
//
// `flush` must not be called while holding a `Shared::db` lock or a write transaction.

/// Every open redb database, in the order that they were opened.
static OPEN_DATABASES: LazyLock<Mutex<Vec<Weak<Shared>>>> = LazyLock::new(Default::default);
/// Held while flushing, and while a database is dropped or starts compacting.
static FLUSH_LOCK: Mutex<()> = parking_lot::const_mutex(());

struct Shared {
    db: RwLock<redb::Database>,
    /// Whether this database has commits that are not yet durable.
    dirty: AtomicBool,
    /// Whether this database is being compacted, during which it has no commits to flush.
    compacting: AtomicBool,
}

pub struct Redb {
    shared: Arc<Shared>,
}

impl Redb {
    pub fn open(path: &Path) -> Result<Self, Error> {
        Self::from_database(create_or_upgrade(&path.join(DB_FILE_NAME))?)
    }

    fn from_database(db: redb::Database) -> Result<Self, Error> {
        // Create all tables in a single transaction so that the allocator state is only written
        // once (see `begin_write`).
        let tx = Self::begin_write(&db)?;
        for column in DBColumn::iter() {
            tx.open_table(Self::table_definition(column))?;
        }
        tx.commit()?;

        let shared = Arc::new(Shared {
            db: RwLock::new(db),
            dirty: AtomicBool::new(false),
            compacting: AtomicBool::new(false),
        });
        OPEN_DATABASES.lock().push(Arc::downgrade(&shared));

        Ok(Self { shared })
    }

    /// Begin a write transaction with redb's quick-repair enabled.
    ///
    /// Quick-repair persists the allocator state with every commit, so that the database can be
    /// reopened quickly after an unclean shutdown. Without it, redb has to re-read the entire
    /// database file to rebuild the allocator state, which can take hours for large databases.
    ///
    /// Every write transaction must use quick-repair: a durable commit without it deletes the
    /// saved allocator state.
    fn begin_write(db: &redb::Database) -> Result<redb::WriteTransaction, Error> {
        let mut tx = db.begin_write()?;
        tx.set_quick_repair(true);
        Ok(tx)
    }

    /// Begin a write transaction whose commit is made durable later, by `flush`.
    fn begin_unflushed_write(db: &redb::Database) -> Result<redb::WriteTransaction, Error> {
        let mut tx = Self::begin_write(db)?;
        tx.set_durability(redb::Durability::None)?;
        Ok(tx)
    }

    /// Commit a transaction from `begin_unflushed_write`.
    fn commit(&self, tx: redb::WriteTransaction) -> Result<(), Error> {
        // Set before committing, so that a flush, which waits for this transaction, sees it.
        self.shared.dirty.store(true, Ordering::Release);
        tx.commit().map_err(Into::into)
    }

    fn table_definition(
        column: DBColumn,
    ) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
        TableDefinition::new(column.into())
    }

    pub fn write_options(&self) -> WriteOptions {
        WriteOptions::new()
    }

    pub fn write_options_sync(&self) -> WriteOptions {
        let mut opts = WriteOptions::new();
        opts.sync = true;
        opts
    }

    pub fn put_bytes_with_options(
        &self,
        col: DBColumn,
        key: &[u8],
        val: &[u8],
        opts: WriteOptions,
    ) -> Result<(), Error> {
        metrics::inc_counter_vec(&metrics::DISK_DB_WRITE_COUNT, &[col.into()]);
        metrics::inc_counter_vec_by(
            &metrics::DISK_DB_WRITE_BYTES,
            &[col.into()],
            val.len() as u64,
        );
        let timer = metrics::start_timer(&metrics::DISK_DB_WRITE_TIMES);

        let table_definition = Self::table_definition(col);
        let open_db = self.shared.db.read();
        let tx = Self::begin_unflushed_write(&open_db)?;
        let mut table = tx.open_table(table_definition)?;

        table.insert(key, val).map(|_| {
            metrics::stop_timer(timer);
        })?;
        drop(table);
        self.commit(tx)?;
        drop(open_db);

        if opts.sync {
            flush()?;
        }
        Ok(())
    }

    /// Store some `value` in `column`, indexed with `key`.
    pub fn put_bytes(&self, col: DBColumn, key: &[u8], val: &[u8]) -> Result<(), Error> {
        self.put_bytes_with_options(col, key, val, self.write_options())
    }

    pub fn put_bytes_sync(&self, col: DBColumn, key: &[u8], val: &[u8]) -> Result<(), Error> {
        self.put_bytes_with_options(col, key, val, self.write_options_sync())
    }

    /// Make all writes to every redb database durable.
    pub fn sync(&self) -> Result<(), Error> {
        flush()
    }

    // Retrieve some bytes in `column` with `key`.
    pub fn get_bytes(&self, col: DBColumn, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        metrics::inc_counter_vec(&metrics::DISK_DB_READ_COUNT, &[col.into()]);
        let timer = metrics::start_timer(&metrics::DISK_DB_READ_TIMES);

        let table_definition = Self::table_definition(col);
        let open_db = self.shared.db.read();
        let tx = open_db.begin_read()?;
        let table = tx.open_table(table_definition)?;

        let result = table.get(key)?;

        match result {
            Some(access_guard) => {
                let value = access_guard.value().to_vec();
                metrics::inc_counter_vec_by(
                    &metrics::DISK_DB_READ_BYTES,
                    &[col.into()],
                    value.len() as u64,
                );
                metrics::stop_timer(timer);
                Ok(Some(value))
            }
            None => {
                metrics::stop_timer(timer);
                Ok(None)
            }
        }
    }

    /// Return `true` if `key` exists in `column`.
    pub fn key_exists(&self, col: DBColumn, key: &[u8]) -> Result<bool, Error> {
        metrics::inc_counter_vec(&metrics::DISK_DB_EXISTS_COUNT, &[col.into()]);

        let table_definition = Self::table_definition(col);
        let open_db = self.shared.db.read();
        let tx = open_db.begin_read()?;
        let table = tx.open_table(table_definition)?;

        table
            .get(key)
            .map_err(Into::into)
            .map(|access_guard| access_guard.is_some())
    }

    /// Removes `key` from `column`.
    pub fn key_delete(&self, col: DBColumn, key: &[u8]) -> Result<(), Error> {
        let table_definition = Self::table_definition(col);
        let open_db = self.shared.db.read();
        let tx = Self::begin_unflushed_write(&open_db)?;
        let mut table = tx.open_table(table_definition)?;
        metrics::inc_counter_vec(&metrics::DISK_DB_DELETE_COUNT, &[col.into()]);

        table.remove(key).map(|_| ())?;
        drop(table);
        self.commit(tx)
    }

    pub fn do_atomically(&self, mut ops_batch: Vec<KeyValueStoreOp>) -> Result<(), Error> {
        // redb writes keys in ascending order faster and more compactly than in random order. The
        // sort is stable, so ops on the same key are applied in their original order.
        ops_batch.sort_by(|a, b| Self::op_column_and_key(a).cmp(&Self::op_column_and_key(b)));

        let open_db = self.shared.db.read();
        let tx = Self::begin_unflushed_write(&open_db)?;
        let mut open_table: Option<(&'static str, BytesTable<'_>)> = None;
        for op in ops_batch {
            let (column_name, _) = Self::op_column_and_key(&op);
            let table = match &mut open_table {
                Some((name, table)) if *name == column_name => table,
                _ => {
                    open_table = None;
                    let table = tx.open_table(TableDefinition::new(column_name))?;
                    &mut open_table.insert((column_name, table)).1
                }
            };

            match op {
                KeyValueStoreOp::PutKeyValue(column, key, value) => {
                    let _timer = metrics::start_timer(&metrics::DISK_DB_WRITE_TIMES);
                    metrics::inc_counter_vec_by(
                        &metrics::DISK_DB_WRITE_BYTES,
                        &[column.into()],
                        value.len() as u64,
                    );
                    metrics::inc_counter_vec(&metrics::DISK_DB_WRITE_COUNT, &[column.into()]);
                    table.insert(key.as_slice(), value.as_slice())?;
                }

                KeyValueStoreOp::DeleteKey(column, key) => {
                    metrics::inc_counter_vec(&metrics::DISK_DB_DELETE_COUNT, &[column.into()]);
                    let _timer = metrics::start_timer(&metrics::DISK_DB_DELETE_TIMES);
                    table.remove(key.as_slice())?;
                }
            }
        }
        drop(open_table);

        self.commit(tx)
    }

    fn op_column_and_key(op: &KeyValueStoreOp) -> (&'static str, &[u8]) {
        match op {
            KeyValueStoreOp::PutKeyValue(column, key, _)
            | KeyValueStoreOp::DeleteKey(column, key) => ((*column).into(), key),
        }
    }

    /// Compact all values in the states and states flag columns.
    pub fn compact(&self) -> Result<(), Error> {
        let _timer = metrics::start_timer(&metrics::DISK_DB_COMPACT_TIMES);

        // Compaction commits durably, which would make this database durable ahead of the others
        // (see `flush`). Flush with it write-locked, so that it has no commits to make durable.
        let mut open_db = {
            let _flush_guard = FLUSH_LOCK.lock();
            let open_db = self.shared.db.write();
            flush_locked(Some((&self.shared, &open_db)))?;
            self.shared.compacting.store(true, Ordering::Release);
            open_db
        };

        let result = (|| {
            open_db.compact()?;
            // Compaction commits without quick-repair, which deletes the saved allocator state.
            // Re-save it so that an unclean shutdown before the next write doesn't need a full
            // repair.
            Self::begin_write(&open_db)?.commit()?;
            Ok(())
        })();
        // Cleared while write-locked, so that a flush that sees it cleared waits for the lock
        // rather than skipping this database while it may have commits to flush.
        self.shared.compacting.store(false, Ordering::Release);
        drop(open_db);
        result
    }

    pub fn iter_column_keys_from<K: Key>(
        &self,
        column: DBColumn,
        from: &[u8],
    ) -> ColumnKeyIter<'_, K> {
        let table_definition = Self::table_definition(column);

        let result = (|| {
            let open_db = self.shared.db.read();
            let read_txn = open_db.begin_read()?;
            let table = read_txn.open_table(table_definition)?;
            let range = table.range(from..)?;
            Ok(range.map(move |res| {
                let (key, _) = res?;
                metrics::inc_counter_vec(&metrics::DISK_DB_KEY_READ_COUNT, &[column.into()]);
                metrics::inc_counter_vec_by(
                    &metrics::DISK_DB_KEY_READ_BYTES,
                    &[column.into()],
                    key.value().len() as u64,
                );
                K::from_bytes(key.value())
            }))
        })();

        match result {
            Ok(iter) => Box::new(iter),
            Err(err) => Box::new(std::iter::once(Err(err))),
        }
    }

    /// Iterate through all keys and values in a particular column.
    pub fn iter_column_keys<K: Key>(&self, column: DBColumn) -> ColumnKeyIter<'_, K> {
        self.iter_column_keys_from(column, &vec![0; column.key_size()])
    }

    pub fn iter_column_from<K: Key>(&self, column: DBColumn, from: &[u8]) -> ColumnIter<'_, K> {
        let table_definition = Self::table_definition(column);

        let result = (|| {
            let open_db = self.shared.db.read();
            let read_txn = open_db.begin_read()?;
            let table = read_txn.open_table(table_definition)?;
            let range = table.range(from..)?;

            Ok(range.map(move |res| {
                let (key, value) = res?;
                metrics::inc_counter_vec(&metrics::DISK_DB_READ_COUNT, &[column.into()]);
                metrics::inc_counter_vec_by(
                    &metrics::DISK_DB_READ_BYTES,
                    &[column.into()],
                    value.value().len() as u64,
                );
                Ok((K::from_bytes(key.value())?, value.value().to_vec()))
            }))
        })();

        match result {
            Ok(iter) => Box::new(iter),
            Err(err) => Box::new(std::iter::once(Err(err))),
        }
    }

    pub fn iter_column<K: Key>(&self, column: DBColumn) -> ColumnIter<'_, K> {
        self.iter_column_from(column, &vec![0; column.key_size()])
    }

    pub fn delete_batch(&self, col: DBColumn, ops: HashSet<&[u8]>) -> Result<(), Error> {
        let open_db = self.shared.db.read();
        let tx = Self::begin_unflushed_write(&open_db)?;

        let mut table = tx.open_table(Self::table_definition(col))?;
        for key in ops {
            table.remove(key)?;
        }

        drop(table);
        self.commit(tx)
    }

    pub fn delete_if(
        &self,
        column: DBColumn,
        mut f: impl FnMut(&[u8]) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        let open_db = self.shared.db.read();
        let tx = Self::begin_unflushed_write(&open_db)?;

        let table_definition = Self::table_definition(column);

        let mut table = tx.open_table(table_definition)?;
        table.retain(|_, value| !f(value).unwrap_or(false))?;

        drop(table);
        self.commit(tx)
    }
}

impl Drop for Redb {
    fn drop(&mut self) {
        // Flush, and remove this database so that flushes no longer hold a reference to it. It is
        // then closed on this thread when `self.shared` is dropped, instead of possibly on the
        // flush thread, which would leave the file locked after this returns.
        let _flush_guard = FLUSH_LOCK.lock();
        if let Err(error) = flush_locked(None) {
            error!(?error, "Failed to flush redb databases");
        }
        OPEN_DATABASES
            .lock()
            .retain(|shared| !std::ptr::eq(shared.as_ptr(), Arc::as_ptr(&self.shared)));
    }
}

/// Make the commits of every open redb database durable, as of a single point in time.
///
/// Commits are not durable when they return, so that writes don't each wait for the disk. If each
/// database were made durable separately, a crash could leave them at different points in time,
/// for example with a block in the hot database but not its data columns in the blobs database.
/// Instead, a write transaction is opened on every database first, which waits for in-progress
/// writes and blocks new ones, so the databases are durable as of the same point in time.
/// Lighthouse never holds write transactions on two databases at once, so this can't deadlock.
///
/// The first database opened, which `HotColdDB` opens as the hot database, is committed last, and
/// only if the others were committed. A crash part-way through can then leave the hot database
/// older than the others, but not newer. Block import writes the blobs database before the hot
/// database, so it is safe. Blob pruning updates the hot database before deleting from the blobs
/// database, so such a crash can leave the hot database advertising blobs that were deleted, until
/// the next prune.
fn flush() -> Result<(), Error> {
    let _flush_guard = FLUSH_LOCK.lock();
    flush_locked(None)
}

/// `flush`, with `FLUSH_LOCK` held by the caller.
///
/// `held` is a database that the caller has write-locked, with its locked `redb::Database`.
fn flush_locked(held: Option<(&Shared, &redb::Database)>) -> Result<(), Error> {
    let databases = {
        let mut open_databases = OPEN_DATABASES.lock();
        open_databases.retain(|shared| shared.strong_count() > 0);
        open_databases
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>()
    };

    let mut txns = Vec::with_capacity(databases.len());
    for shared in databases.iter().rev() {
        let tx = match held {
            Some((held_shared, held_db)) if std::ptr::eq(held_shared, shared.as_ref()) => {
                Redb::begin_write(held_db)?
            }
            // A database being compacted has no commits to flush, and can't be written to.
            _ if shared.compacting.load(Ordering::Acquire) => continue,
            _ => Redb::begin_write(&shared.db.read())?,
        };
        txns.push((shared, tx));
    }

    // Release the databases without commits to flush, by aborting their transactions, so that
    // writes to them aren't blocked while the others are committed. An empty durable commit makes
    // all earlier commits durable.
    let mut dirty_txns = txns
        .into_iter()
        .filter(|(shared, _)| shared.dirty.swap(false, Ordering::AcqRel))
        .collect::<Vec<_>>();
    let Some((last_shared, last_tx)) = dirty_txns.pop() else {
        return Ok(());
    };

    // Commit all but the last in parallel, since they are independent files.
    let results = std::thread::scope(|scope| {
        let mut handles = vec![];
        let mut results = vec![];
        for (shared, tx) in dirty_txns {
            match std::thread::Builder::new().spawn_scoped(scope, move || commit_flush(shared, tx))
            {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    // `tx` was dropped with the closure, aborting it.
                    shared.dirty.store(true, Ordering::Release);
                    results.push(Err(Error::DBError {
                        message: format!("failed to spawn redb flush thread: {error}"),
                    }));
                }
            }
        }
        for handle in handles {
            results.push(handle.join().unwrap_or_else(|_| {
                Err(Error::DBError {
                    message: "redb flush thread panicked".into(),
                })
            }));
        }
        results
    });

    if let Some(error) = results.into_iter().find_map(Result::err) {
        last_shared.dirty.store(true, Ordering::Release);
        return Err(error);
    }
    commit_flush(last_shared, last_tx)
}

fn commit_flush(shared: &Shared, tx: redb::WriteTransaction) -> Result<(), Error> {
    tx.commit().map_err(|e| {
        shared.dirty.store(true, Ordering::Release);
        e.into()
    })
}

/// Log a full repair's progress, at most once per percent.
///
/// A full repair re-reads the entire database file, which can take hours for large databases.
fn log_repair_progress(path: &str, last_logged: &AtomicU64, progress: f64) {
    // Offset by one so that the first call is logged.
    let marker = (progress * 100.0) as u64 + 1;
    if last_logged.fetch_max(marker, Ordering::Relaxed) < marker {
        info!(path, percent = marker - 1, "Repairing redb database");
    }
}

fn builder(path: &Path) -> redb::Builder {
    let path = path.display().to_string();
    let last_logged = AtomicU64::new(0);
    let mut builder = redb::Builder::new();
    builder.set_repair_callback(move |session| {
        log_repair_progress(&path, &last_logged, session.progress())
    });
    builder
}

/// Open the database file at `path`, upgrading it from the v2 file format used by redb 2.x if
/// necessary.
fn create_or_upgrade(path: &Path) -> Result<redb::Database, Error> {
    match builder(path).create(path) {
        Err(redb::DatabaseError::UpgradeRequired(version)) => {
            let path_str = path.display().to_string();
            warn!(
                path = path_str,
                version,
                "Upgrading redb database file format. This is irreversible and may take hours"
            );
            let log_path = path_str.clone();
            let last_logged = AtomicU64::new(0);
            redb_v2::Builder::new()
                .set_repair_callback(move |session| {
                    log_repair_progress(&log_path, &last_logged, session.progress())
                })
                .open(path)?
                .upgrade()?;
            info!(path = path_str, "Upgraded redb database file format");
            // The upgrade does not save the allocator state, so this open does a full repair.
            builder(path).create(path).map_err(Into::into)
        }
        result => result.map_err(Into::into),
    }
}

/// Serializes tests that open databases, since `flush` acts on all of them.
#[cfg(test)]
static TEST_LOCK: Mutex<()> = parking_lot::const_mutex(());

#[cfg(test)]
mod crash_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{TempDir, tempdir};

    /// Copy the database file as it currently is on disk, which is what a new process would see
    /// if this one crashed now, and check that the copy opens without a full repair.
    #[track_caller]
    fn copy_and_open_without_repair(dir: &Path) -> TempDir {
        let copy_dir = tempdir().unwrap();
        // Prevent a background flush from writing to the file while it is copied.
        let flush_guard = FLUSH_LOCK.lock();
        std::fs::copy(dir.join(DB_FILE_NAME), copy_dir.path().join(DB_FILE_NAME)).unwrap();
        drop(flush_guard);

        // redb invokes the repair callback before starting a full repair (which re-reads the whole
        // file), and aborting it fails the open.
        redb::Builder::new()
            .set_repair_callback(|session| session.abort())
            .create(copy_dir.path().join(DB_FILE_NAME))
            .expect("full repair should not be required");
        copy_dir
    }

    /// Regression test for https://github.com/sigp/lighthouse/issues/10440.
    ///
    /// Close the database uncleanly by never running `Drop` for the `redb::Database`, as happens
    /// after a crash, and check that it can be reopened without a full repair.
    #[test]
    fn reopen_after_unclean_shutdown_without_repair() {
        let _test_guard = TEST_LOCK.lock();
        let col = DBColumn::BeaconBlock;
        let key = |i: u64| i.to_be_bytes().to_vec();
        let value = |i: u64| i.to_be_bytes().repeat(128);

        let dir = tempdir().unwrap();
        let db = Redb::open(dir.path()).unwrap();
        copy_and_open_without_repair(dir.path());

        // Only the most recent durable commit determines whether a full repair is needed, so check
        // after every write path.
        db.do_atomically(
            (0..1024)
                .map(|i| KeyValueStoreOp::PutKeyValue(col, key(i), value(i)))
                .collect(),
        )
        .unwrap();
        copy_and_open_without_repair(dir.path());

        let (key_0, key_1, value_2) = (key(0), key(1), value(2));
        db.delete_batch(col, HashSet::from([key_0.as_slice(), key_1.as_slice()]))
            .unwrap();
        db.delete_if(col, |val| Ok(val == value_2)).unwrap();
        copy_and_open_without_repair(dir.path());

        db.key_delete(col, &key(3)).unwrap();
        copy_and_open_without_repair(dir.path());

        db.do_atomically(vec![KeyValueStoreOp::DeleteKey(col, key(4))])
            .unwrap();
        copy_and_open_without_repair(dir.path());

        db.put_bytes(col, &key(5), b"put").unwrap();
        copy_and_open_without_repair(dir.path());

        db.put_bytes_sync(col, &key(6), b"put_sync").unwrap();
        copy_and_open_without_repair(dir.path());

        db.compact().unwrap();
        copy_and_open_without_repair(dir.path());

        // Close uncleanly: `Drop for redb::Database` writes the allocator state, so never run it.
        // The leaked database keeps redb's exclusive file lock, so reopen a copy of the file.
        std::mem::forget(db);
        let reopen_dir = copy_and_open_without_repair(dir.path());

        // All writes made before the unclean shutdown must have been persisted.
        let db = Redb::open(reopen_dir.path()).unwrap();
        for i in 0..5 {
            assert_eq!(db.get_bytes(col, &key(i)).unwrap(), None, "key {i}");
        }
        assert_eq!(db.get_bytes(col, &key(5)).unwrap(), Some(b"put".to_vec()));
        assert_eq!(
            db.get_bytes(col, &key(6)).unwrap(),
            Some(b"put_sync".to_vec())
        );
        for i in 7..1024 {
            assert_eq!(
                db.get_bytes(col, &key(i)).unwrap(),
                Some(value(i)),
                "key {i}"
            );
        }
    }

    #[test]
    fn upgrade_v2_database() {
        let _test_guard = TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let table_name: &str = DBColumn::Dummy.into();
        let table: redb_v2::TableDefinition<'_, &[u8], &[u8]> =
            redb_v2::TableDefinition::new(table_name);

        let db = redb_v2::Database::create(dir.path().join(DB_FILE_NAME)).unwrap();
        let tx = db.begin_write().unwrap();
        tx.open_table(table)
            .unwrap()
            .insert(b"key".as_slice(), b"value".as_slice())
            .unwrap();
        tx.commit().unwrap();
        drop(db);

        let db = Redb::open(dir.path()).unwrap();
        assert_eq!(
            db.get_bytes(DBColumn::Dummy, b"key").unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[test]
    fn do_atomically_preserves_order_of_ops_on_same_key() {
        let _test_guard = TEST_LOCK.lock();
        let dir = tempdir().unwrap();
        let db = Redb::open(dir.path()).unwrap();
        let (hot, cold) = (DBColumn::BeaconBlock, DBColumn::BeaconState);
        let put = |col, key: &[u8], value: &[u8]| {
            KeyValueStoreOp::PutKeyValue(col, key.to_vec(), value.to_vec())
        };
        let delete = |col, key: &[u8]| KeyValueStoreOp::DeleteKey(col, key.to_vec());

        db.do_atomically(vec![
            put(hot, b"b", b"1"),
            put(cold, b"a", b"1"),
            delete(hot, b"b"),
            put(hot, b"a", b"1"),
            delete(cold, b"c"),
            put(cold, b"c", b"1"),
            put(hot, b"a", b"2"),
        ])
        .unwrap();

        assert_eq!(db.get_bytes(hot, b"a").unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.get_bytes(hot, b"b").unwrap(), None);
        assert_eq!(db.get_bytes(cold, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get_bytes(cold, b"c").unwrap(), Some(b"1".to_vec()));
    }
}
