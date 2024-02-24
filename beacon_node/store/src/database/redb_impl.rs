use crate::{metrics, ColumnIter, ColumnKeyIter, Key, RawEntryIter, RawKeyIter};
use crate::{DBColumn, Error, KeyValueStoreOp};
use parking_lot::{Mutex, MutexGuard, RwLock};
use redb::{ReadableTable, TableDefinition};
use std::{borrow::BorrowMut, marker::PhantomData, path::Path};
use strum::IntoEnumIterator;
use types::{EthSpec, Hash256};

use super::interface::WriteOptions;

pub const DB_FILE_NAME: &str = "database";

pub struct Redb<E: EthSpec> {
    db: RwLock<redb::Database>,
    transaction_mutex: Mutex<()>,
    _phantom: PhantomData<E>,
}

impl From<WriteOptions> for redb::Durability {
    fn from(options: WriteOptions) -> Self {
        if options.sync {
            redb::Durability::Immediate
        } else {
            redb::Durability::Eventual
        }
    }
}

impl<E: EthSpec> Redb<E> {
    pub fn open(path: &Path) -> Result<Self, Error> {
        let path = if path.is_dir() {
            path.join(DB_FILE_NAME)
        } else {
            path.to_path_buf()
        };
        let db = redb::Database::create(path)?;
        let transaction_mutex = Mutex::new(());

        for column in DBColumn::iter() {
            Redb::<E>::create_table(&db, column.into())?;
        }

        Ok(Self {
            db: db.into(),
            transaction_mutex,
            _phantom: PhantomData,
        })
    }

    fn create_table(db: &redb::Database, table_name: &str) -> Result<(), Error> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(table_name);
        let tx = db.begin_write()?;
        tx.open_table(table_definition)?;
        tx.commit().map_err(Into::into)
    }

    pub fn write_options(&self) -> WriteOptions {
        WriteOptions::new()
    }

    pub fn write_options_sync(&self) -> WriteOptions {
        let mut opts = WriteOptions::new();
        opts.sync = true;
        opts
    }

    pub fn begin_rw_transaction(&self) -> MutexGuard<()> {
        self.transaction_mutex.lock()
    }

    pub fn put_bytes_with_options(
        &self,
        col: &str,
        key: &[u8],
        val: &[u8],
        opts: WriteOptions,
    ) -> Result<(), Error> {
        metrics::inc_counter(&metrics::DISK_DB_WRITE_COUNT);
        metrics::inc_counter_by(&metrics::DISK_DB_WRITE_BYTES, val.len() as u64);
        let timer = metrics::start_timer(&metrics::DISK_DB_WRITE_TIMES);

        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(col);
        let open_db = self.db.read();
        let mut tx = open_db.begin_write()?;
        tx.set_durability(opts.into());
        let mut table = tx.open_table(table_definition)?;

        table.insert(key, val).map(|_| {
            metrics::stop_timer(timer);
        })?;
        drop(table);
        tx.commit().map_err(Into::into)
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
        metrics::inc_counter(&metrics::DISK_DB_READ_COUNT);
        let timer = metrics::start_timer(&metrics::DISK_DB_READ_TIMES);

        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(col);
        let open_db = self.db.read();
        let tx = open_db.begin_read()?;
        let table = tx.open_table(table_definition)?;

        let result = table.get(key)?;

        // TODO: clean this up
        if let Some(access_guard) = result {
            let value = access_guard.value().to_vec();
            metrics::inc_counter_by(&metrics::DISK_DB_READ_BYTES, value.len() as u64);
            metrics::stop_timer(timer);
            Ok(Some(value))
        } else {
            metrics::inc_counter_by(&metrics::DISK_DB_READ_BYTES, 0_u64);
            metrics::stop_timer(timer);
            Ok(None)
        }
    }

    /// Return `true` if `key` exists in `column`.
    pub fn key_exists(&self, col: &str, key: &[u8]) -> Result<bool, Error> {
        metrics::inc_counter(&metrics::DISK_DB_EXISTS_COUNT);

        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(col);
        let open_db = self.db.read();
        let tx = open_db.begin_read()?;
        let table = tx.open_table(table_definition)?;

        table
            .get(key)
            .map_err(Into::into)
            .map(|access_guard| access_guard.is_some())
    }

    /// Removes `key` from `column`.
    pub fn key_delete(&self, col: &str, key: &[u8]) -> Result<(), Error> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(col);
        let open_db = self.db.read();
        let tx = open_db.begin_write()?;
        let mut table = tx.open_table(table_definition)?;

        metrics::inc_counter(&metrics::DISK_DB_DELETE_COUNT);

        table.remove(key).map(|_| ())?;
        drop(table);
        tx.commit().map_err(Into::into)
    }

    // TODO we need some way to fetch the correct table
    pub fn do_atomically(&self, ops_batch: Vec<KeyValueStoreOp>) -> Result<(), Error> {
        let open_db = self.db.read();
        let mut tx = open_db.begin_write()?;
        tx.set_durability(self.write_options().into());
        for op in ops_batch {
            match op {
                KeyValueStoreOp::PutKeyValue(column, key, value) => {
                    let table_definition: TableDefinition<'_, &[u8], &[u8]> =
                        TableDefinition::new(&column);

                    let mut table = tx.open_table(table_definition)?;
                    table.insert(key.as_slice(), value.as_slice())?;
                    drop(table);
                }

                KeyValueStoreOp::DeleteKey(column, key) => {
                    let table_definition: TableDefinition<'_, &[u8], &[u8]> =
                        TableDefinition::new(&column);

                    let mut table = tx.open_table(table_definition)?;
                    table.remove(key.as_slice())?;
                    drop(table);
                }
            }
        }

        tx.commit()?;
        Ok(())
    }

    /// Compact all values in the states and states flag columns.
    pub fn compact(&self) -> Result<(), Error> {
        let mut open_db = self.db.write();
        let mut_db = open_db.borrow_mut();
        mut_db.compact().map_err(Into::into).map(|_| ())
    }

    pub fn iter_column_keys_from<K: Key>(&self, column: DBColumn, from: &[u8]) -> ColumnKeyIter<K> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> =
            TableDefinition::new(column.into());

        let iter = {
            let open_db = self.db.read();
            let read_txn = open_db.begin_read()?;
            let table = read_txn.open_table(table_definition)?;
            table.range(from..)?.map(|res| {
                let (k, _) = res?;
                K::from_bytes(k.value())
            })
        };

        Ok(Box::new(iter))
    }

    /// Iterate through all keys and values in a particular column.
    pub fn iter_column_keys<K: Key>(&self, column: DBColumn) -> ColumnKeyIter<K> {
        self.iter_column_keys_from(column, Hash256::zero().as_bytes())
    }

    pub fn iter_column_from<K: Key>(&self, column: DBColumn, from: &[u8]) -> ColumnIter<K> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> =
            TableDefinition::new(column.into());

        let iter = {
            let open_db = self.db.read();
            let read_txn = open_db.begin_read()?;
            let table = read_txn.open_table(table_definition)?;
            table.range(from..)?.map(|res| {
                let (k, v) = res?;
                Ok((K::from_bytes(k.value())?, v.value().to_vec()))
            })
        };

        Ok(Box::new(iter))
    }

    pub fn iter_column<K: Key>(&self, column: DBColumn) -> ColumnIter<K> {
        self.iter_column_from(column, &vec![0; column.key_size()])
    }

    pub fn iter_raw_keys(&self, column: DBColumn, prefix: &[u8]) -> RawKeyIter {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> =
            TableDefinition::new(column.into());

        let iter = {
            let open_db = self.db.read();
            let read_txn = open_db.begin_read()?;
            let table = read_txn.open_table(table_definition)?;
            table.range(prefix..)?.map(|res| {
                let (k, _) = res?;
                Ok(k.value().to_vec())
            })
        };

        Ok(Box::new(iter))
    }

    /// Return an iterator over the state roots of all temporary states.
    pub fn iter_temporary_state_roots(
        &self,
        column: DBColumn,
    ) -> Result<impl Iterator<Item = Result<Hash256, Error>> + '_, Error> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> =
            TableDefinition::new(column.into());

        let iter = {
            let open_db = self.db.read();
            let read_txn = open_db.begin_read()?;
            let table = read_txn.open_table(table_definition)?;
            table.range(Hash256::zero().as_bytes()..)?.map(|res| {
                let (k, _) = res?;
                Hash256::from_bytes(k.value())
            })
        };

        Ok(iter)
    }
}
