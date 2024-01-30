#[cfg(feature = "leveldb")]
use crate::hot_cold_store::{BytesKey, HotColdDBError};
use crate::Key;
use crate::{
    get_key_for_col, metrics, ColumnIter, ColumnKeyIter, DBColumn, Error, ItemStore, KeyValueStore,
    KeyValueStoreOp, RawEntryIter, RawKeyIter,
};
use leveldb::compaction::Compaction;
use leveldb::database::batch::{Batch, Writebatch};
use leveldb::database::kv::KV;
use leveldb::database::Database;
use leveldb::iterator::{Iterable, LevelDBIterator};
use leveldb::options::{Options, ReadOptions};
use parking_lot::{Mutex, MutexGuard};
use std::marker::PhantomData;
use std::path::Path;
use types::{EthSpec, Hash256};

use super::interface::WriteOptions;

pub struct LevelDB<E: EthSpec> {
    db: Database<BytesKey>,
    /// A mutex to synchronise sensitive read-write transactions.
    transaction_mutex: Mutex<()>,
    _phantom: PhantomData<E>,
}

impl From<WriteOptions> for leveldb::options::WriteOptions {
    fn from(options: WriteOptions) -> Self {
        // Assuming LevelDBWriteOptions has a new method that accepts a bool parameter for sync.
        let mut opts = leveldb::options::WriteOptions::new();
        opts.sync = options.sync;
        opts
    }
}

impl<E: EthSpec> LevelDB<E> {
    pub fn open(path: &Path) -> Result<Self, Error> {
        let mut options = Options::new();

        options.create_if_missing = true;

        let db = Database::open(path, options)?;
        let transaction_mutex = Mutex::new(());

        Ok(Self {
            db,
            transaction_mutex,
            _phantom: PhantomData,
        })
    }

    pub fn read_options(&self) -> ReadOptions<BytesKey> {
        ReadOptions::new()
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
        col: &str,
        key: &[u8],
        val: &[u8],
        opts: WriteOptions,
    ) -> Result<(), Error> {
        let column_key = get_key_for_col(col, key);

        metrics::inc_counter(&metrics::DISK_DB_WRITE_COUNT);
        metrics::inc_counter_by(&metrics::DISK_DB_WRITE_BYTES, val.len() as u64);
        let timer = metrics::start_timer(&metrics::DISK_DB_WRITE_TIMES);

        self.db
            .put(opts.into(), BytesKey::from_vec(column_key), val)
            .map_err(Into::into)
            .map(|()| {
                metrics::stop_timer(timer);
            })
    }

    /// Store some `value` in `column`, indexed with `key`.
    pub fn put_bytes(&self, col: &str, key: &[u8], val: &[u8]) -> Result<(), Error> {
        self.put_bytes_with_options(col, key, val, self.write_options())
    }

    pub fn put_bytes_sync(&self, col: &str, key: &[u8], val: &[u8]) -> Result<(), Error> {
        self.put_bytes_with_options(col, key, val, self.write_options_sync())
    }

    pub fn sync(&self) -> Result<(), Error> {
        self.put_bytes_sync("sync", b"sync", b"sync")
    }

    // Retrieve some bytes in `column` with `key`.
    pub fn get_bytes(&self, col: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let column_key = get_key_for_col(col, key);

        metrics::inc_counter(&metrics::DISK_DB_READ_COUNT);
        let timer = metrics::start_timer(&metrics::DISK_DB_READ_TIMES);

        self.db
            .get(self.read_options(), BytesKey::from_vec(column_key))
            .map_err(Into::into)
            .map(|opt| {
                opt.map(|bytes| {
                    metrics::inc_counter_by(&metrics::DISK_DB_READ_BYTES, bytes.len() as u64);
                    metrics::stop_timer(timer);
                    bytes
                })
            })
    }

    /// Return `true` if `key` exists in `column`.
    pub fn key_exists(&self, col: &str, key: &[u8]) -> Result<bool, Error> {
        let column_key = get_key_for_col(col, key);

        metrics::inc_counter(&metrics::DISK_DB_EXISTS_COUNT);

        self.db
            .get(self.read_options(), BytesKey::from_vec(column_key))
            .map_err(Into::into)
            .map(|val| val.is_some())
    }

    /// Removes `key` from `column`.
    pub fn key_delete(&self, col: &str, key: &[u8]) -> Result<(), Error> {
        let column_key = get_key_for_col(col, key);

        metrics::inc_counter(&metrics::DISK_DB_DELETE_COUNT);

        self.db
            .delete(self.write_options().into(), BytesKey::from_vec(column_key))
            .map_err(Into::into)
    }

    pub fn do_atomically(&self, ops_batch: Vec<KeyValueStoreOp>) -> Result<(), Error> {
        let mut leveldb_batch = Writebatch::new();
        for op in ops_batch {
            match op {
                KeyValueStoreOp::PutKeyValue(key, value) => {
                    leveldb_batch.put(BytesKey::from_vec(key), &value);
                }

                KeyValueStoreOp::DeleteKey(key) => {
                    leveldb_batch.delete(BytesKey::from_vec(key));
                }
            }
        }
        self.db.write(self.write_options().into(), &leveldb_batch)?;
        Ok(())
    }

    pub fn begin_rw_transaction(&self) -> MutexGuard<()> {
        self.transaction_mutex.lock()
    }

    /// Compact all values in the states and states flag columns.
    pub fn compact(&self) -> Result<(), Error> {
        let endpoints = |column: DBColumn| {
            (
                BytesKey::from_vec(get_key_for_col(column.as_str(), Hash256::zero().as_bytes())),
                BytesKey::from_vec(get_key_for_col(
                    column.as_str(),
                    Hash256::repeat_byte(0xff).as_bytes(),
                )),
            )
        };

        for (start_key, end_key) in [
            endpoints(DBColumn::BeaconStateTemporary),
            endpoints(DBColumn::BeaconState),
            endpoints(DBColumn::BeaconStateSummary),
        ] {
            self.db.compact(&start_key, &end_key);
        }
        Ok(())
    }

    pub fn iter_column_from<K: Key>(&self, column: DBColumn, from: &[u8]) -> ColumnIter<K> {
        let start_key = BytesKey::from_vec(get_key_for_col(column.into(), from));

        let iter = self.db.iter(self.read_options());
        iter.seek(&start_key);

        Box::new(
            iter.take_while(move |(key, _)| key.matches_column(column))
                .map(move |(bytes_key, value)| {
                    let key = bytes_key.remove_column_variable(column).ok_or_else(|| {
                        HotColdDBError::IterationError {
                            unexpected_key: bytes_key.clone(),
                        }
                    })?;
                    Ok((K::from_bytes(key)?, value))
                }),
        )
    }

    /// Iterate through all keys and values in a particular column.
    pub fn iter_column_keys<K: Key>(&self, column: DBColumn) -> ColumnKeyIter<K> {
        let start_key =
            BytesKey::from_vec(get_key_for_col(column.into(), &vec![0; column.key_size()]));

        let iter = self.db.keys_iter(self.read_options());
        iter.seek(&start_key);

        Box::new(
            iter.take_while(move |key| key.matches_column(column))
                .map(move |bytes_key| {
                    let key = bytes_key.remove_column_variable(column).ok_or_else(|| {
                        HotColdDBError::IterationError {
                            unexpected_key: bytes_key.clone(),
                        }
                    })?;
                    K::from_bytes(key)
                }),
        )
    }

    /// Return an iterator over the state roots of all temporary states.
    pub fn iter_temporary_state_roots(
        &self,
        column: DBColumn,
    ) -> impl Iterator<Item = Result<Hash256, Error>> + '_ {
        let start_key =
            BytesKey::from_vec(get_key_for_col(column.into(), Hash256::zero().as_bytes()));

        let keys_iter = self.db.keys_iter(self.read_options());
        keys_iter.seek(&start_key);

        keys_iter
            .take_while(move |key| key.matches_column(column))
            .map(move |bytes_key| {
                bytes_key.remove_column(column).ok_or_else(|| {
                    HotColdDBError::IterationError {
                        unexpected_key: bytes_key,
                    }
                    .into()
                })
            })
    }

    pub fn iter_column<K: Key>(&self, column: DBColumn) -> ColumnIter<K> {
        self.iter_column_from(column, &vec![0; column.key_size()])
    }
}

impl<E: EthSpec> KeyValueStore<E> for LevelDB<E> {
    fn get_bytes(&self, col: &str, key: &[u8]) -> Result<Option<Vec<u8>>, crate::Error> {
        let column_key = get_key_for_col(col, key);

        metrics::inc_counter(&metrics::DISK_DB_READ_COUNT);
        let timer = metrics::start_timer(&metrics::DISK_DB_READ_TIMES);

        self.db
            .get(self.read_options(), BytesKey::from_vec(column_key))
            .map_err(Into::into)
            .map(|opt| {
                opt.map(|bytes| {
                    metrics::inc_counter_by(&metrics::DISK_DB_READ_BYTES, bytes.len() as u64);
                    metrics::stop_timer(timer);
                    bytes
                })
            })
    }

    fn put_bytes(&self, col: &str, key: &[u8], val: &[u8]) -> Result<(), crate::Error> {
        self.put_bytes_with_options(col, key, val, self.write_options())
    }

    fn put_bytes_sync(&self, col: &str, key: &[u8], val: &[u8]) -> Result<(), crate::Error> {
        self.put_bytes_with_options(col, key, val, self.write_options_sync())
    }

    fn sync(&self) -> Result<(), crate::Error> {
        self.put_bytes_sync("sync", b"sync", b"sync")
    }

    fn key_exists(&self, col: &str, key: &[u8]) -> Result<bool, crate::Error> {
        let column_key = get_key_for_col(col, key);

        metrics::inc_counter(&metrics::DISK_DB_EXISTS_COUNT);

        self.db
            .get(self.read_options(), BytesKey::from_vec(column_key))
            .map_err(Into::into)
            .map(|val| val.is_some())
    }

    fn key_delete(&self, col: &str, key: &[u8]) -> Result<(), crate::Error> {
        let column_key = get_key_for_col(col, key);

        metrics::inc_counter(&metrics::DISK_DB_DELETE_COUNT);

        self.db
            .delete(self.write_options().into(), BytesKey::from_vec(column_key))
            .map_err(Into::into)
    }

    fn do_atomically(&self, ops_batch: Vec<KeyValueStoreOp>) -> Result<(), crate::Error> {
        let mut leveldb_batch = Writebatch::new();
        for op in ops_batch {
            match op {
                KeyValueStoreOp::PutKeyValue(key, value) => {
                    leveldb_batch.put(BytesKey::from_vec(key), &value);
                }

                KeyValueStoreOp::DeleteKey(key) => {
                    leveldb_batch.delete(BytesKey::from_vec(key));
                }
            }
        }
        self.db.write(self.write_options().into(), &leveldb_batch)?;
        Ok(())
    }

    fn begin_rw_transaction(&self) -> MutexGuard<()> {
        self.transaction_mutex.lock()
    }

    fn compact(&self) -> Result<(), crate::Error> {
        let endpoints = |column: DBColumn| {
            (
                BytesKey::from_vec(get_key_for_col(column.as_str(), Hash256::zero().as_bytes())),
                BytesKey::from_vec(get_key_for_col(
                    column.as_str(),
                    Hash256::repeat_byte(0xff).as_bytes(),
                )),
            )
        };

        for (start_key, end_key) in [
            endpoints(DBColumn::BeaconStateTemporary),
            endpoints(DBColumn::BeaconState),
            endpoints(DBColumn::BeaconStateSummary),
        ] {
            self.db.compact(&start_key, &end_key);
        }
        Ok(())
    }

    fn iter_column_from<K: Key>(&self, column: DBColumn, from: &[u8]) -> ColumnIter<K> {
        let start_key = BytesKey::from_vec(get_key_for_col(column.into(), from));

        let iter = self.db.iter(self.read_options());
        iter.seek(&start_key);

        Box::new(
            iter.take_while(move |(key, _)| key.matches_column(column))
                .map(move |(bytes_key, value)| {
                    let key = bytes_key.remove_column_variable(column).ok_or_else(|| {
                        HotColdDBError::IterationError {
                            unexpected_key: bytes_key.clone(),
                        }
                    })?;
                    Ok((K::from_bytes(key)?, value))
                }),
        )
    }

    fn iter_raw_entries(&self, column: DBColumn, prefix: &[u8]) -> RawEntryIter {
        let start_key = BytesKey::from_vec(get_key_for_col(column.into(), prefix));

        let iter = self.db.iter(self.read_options());
        iter.seek(&start_key);

        Box::new(
            iter.take_while(move |(key, _)| key.key.starts_with(start_key.key.as_slice()))
                .map(move |(bytes_key, value)| {
                    let subkey = &bytes_key.key[column.as_bytes().len()..];
                    Ok((Vec::from(subkey), value))
                }),
        )
    }

    fn iter_raw_keys(&self, column: DBColumn, prefix: &[u8]) -> RawKeyIter {
        let start_key = BytesKey::from_vec(get_key_for_col(column.into(), prefix));

        let iter = self.db.keys_iter(self.read_options());
        iter.seek(&start_key);

        Box::new(
            iter.take_while(move |key| key.key.starts_with(start_key.key.as_slice()))
                .map(move |bytes_key| {
                    let subkey = &bytes_key.key[column.as_bytes().len()..];
                    Ok(Vec::from(subkey))
                }),
        )
    }

    /// Iterate through all keys and values in a particular column.
    fn iter_column_keys<K: Key>(&self, column: DBColumn) -> ColumnKeyIter<K> {
        let start_key =
            BytesKey::from_vec(get_key_for_col(column.into(), &vec![0; column.key_size()]));

        let iter = self.db.keys_iter(self.read_options());
        iter.seek(&start_key);

        Box::new(
            iter.take_while(move |key| key.matches_column(column))
                .map(move |bytes_key| {
                    let key = bytes_key.remove_column_variable(column).ok_or_else(|| {
                        HotColdDBError::IterationError {
                            unexpected_key: bytes_key.clone(),
                        }
                    })?;
                    K::from_bytes(key)
                }),
        )
    }
}

impl<E: EthSpec> ItemStore<E> for LevelDB<E> {}
