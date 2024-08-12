use crate::{
    get_key_for_col,
    hot_cold_store::{BytesKey, HotColdDBError},
    metrics, ColumnKeyIter, Key, RawKeyIter,
};
use crate::{DBColumn, Error, KeyValueStoreOp};
use redb::{ReadableTable, TableDefinition};
use std::{f64::consts::E, marker::PhantomData, path::Path};
use strum::IntoEnumIterator;
use types::{EthSpec, Hash256};
use parking_lot::{Mutex, MutexGuard};

use super::interface::WriteOptions;

const TABLE_NAME: &str = "TABLE";
pub const REDB_DATA_FILENAME: &str = "slasher.redb";

pub struct Redb<E: EthSpec> {
    db: redb::Database,
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
        let db_path = path.join(REDB_DATA_FILENAME);
        let db = redb::Database::create(db_path)?;
        let transaction_mutex = Mutex::new(());

        Redb::<E>::create_table(&db, TABLE_NAME)?;

        Ok(Self {
            db,
            transaction_mutex,
            _phantom: PhantomData,
        })
    }

    fn create_table(db: &redb::Database, table_name: &str) -> Result<(), Error> {
        println!("{:?}", table_name);
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
        println!("put_bytes_with_options");
        metrics::inc_counter_vec(&metrics::DISK_DB_WRITE_COUNT, &[col]);
        metrics::inc_counter_vec_by(&metrics::DISK_DB_WRITE_BYTES, &[col], val.len() as u64);        let timer = metrics::start_timer(&metrics::DISK_DB_WRITE_TIMES);
        let column_key = get_key_for_col(col, key);
        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(TABLE_NAME);
        let mut tx = self.db.begin_write()?;
        tx.set_durability(opts.into());
        let mut table = tx.open_table(table_definition)?;

        table.insert(column_key.as_slice(), val).map(|_| {
            metrics::stop_timer(timer);
        })?;
        drop(table);
        tx.commit().map_err(Into::into)
    }

    /// Store some `value` in `column`, indexed with `key`.
    pub fn put_bytes(&self, col: &str, key: &[u8], val: &[u8]) -> Result<(), Error> {
        println!("put_bytes");
        self.put_bytes_with_options(col, key, val, self.write_options())
    }

    pub fn put_bytes_sync(&self, col: &str, key: &[u8], val: &[u8]) -> Result<(), Error> {
        println!("put_bytes_sync");
        self.put_bytes_with_options(col, key, val, self.write_options_sync())
    }

    pub fn sync(&self) -> Result<(), Error> {
        self.put_bytes_sync("sync", b"sync", b"sync")
    }

    // Retrieve some bytes in `column` with `key`.
    pub fn get_bytes(&self, col: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        println!("get_bytes");
        metrics::inc_counter_vec(&metrics::DISK_DB_READ_COUNT, &[col]);
        let timer = metrics::start_timer(&metrics::DISK_DB_READ_TIMES);
        let column_key = get_key_for_col(col, key);

        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(TABLE_NAME);
        let tx = self.db.begin_read()?;
        let table = tx.open_table(table_definition)?;

        let result = table.get(column_key.as_slice())?;

        // TODO: clean this up
        if let Some(access_guard) = result {
            let value = access_guard.value().to_vec();
            metrics::inc_counter_vec_by(
                &metrics::DISK_DB_READ_BYTES,
                &[col],
                value.len() as u64,
            );
            drop(timer);
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// Return `true` if `key` exists in `column`.
    pub fn key_exists(&self, col: &str, key: &[u8]) -> Result<bool, Error> {
        println!("key_exists");
        metrics::inc_counter_vec(&metrics::DISK_DB_EXISTS_COUNT, &[col]);
        let column_key = get_key_for_col(col, key);

        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(TABLE_NAME);
        let tx = self.db.begin_read()?;
        let table = tx.open_table(table_definition)?;

        table
            .get(column_key.as_slice())
            .map_err(Into::into)
            .map(|access_guard| access_guard.is_some())
    }

    /// Removes `key` from `column`.
    pub fn key_delete(&self, col: &str, key: &[u8]) -> Result<(), Error> {
        println!("key_delete");
        let column_key = get_key_for_col(col, key);
        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(TABLE_NAME);
        let tx = self.db.begin_write()?;
        let mut table = tx.open_table(table_definition)?;

        metrics::inc_counter_vec(&metrics::DISK_DB_DELETE_COUNT, &[col]);

        table.remove(column_key.as_slice()).map(|_| ())?;
        drop(table);
        tx.commit().map_err(Into::into)
    }

    // TODO we need some way to fetch the correct table
    pub fn do_atomically(&self, ops_batch: Vec<KeyValueStoreOp>) -> Result<(), Error> {
        println!("do_atomically");
        let table_definition: TableDefinition<'_, &[u8], &[u8]> =
                        TableDefinition::new(TABLE_NAME);
        let tx = self.db.begin_write()?;
        let mut table = tx.open_table(table_definition)?;
        for op in ops_batch {
            match op {
                KeyValueStoreOp::PutKeyValue(column, key, value) => {
                    let column_key = get_key_for_col(&column, &key);
                    table.insert(column_key.as_slice(), value.as_slice())?;
                }

                KeyValueStoreOp::DeleteKey(column, key) => {
                    let column_key = get_key_for_col(&column, &key);
                    table.remove(column_key.as_slice())?;
                }
            }
        }
        drop(table);
        tx.commit()?;
        Ok(())
    }

    /// Compact all values in the states and states flag columns.
    pub fn compact(&self) -> Result<(), Error> {
        // self.db.compact().map_err(Into::into).map(|_| ())
        Ok(())
    }

    pub fn compact_column(&self, _: DBColumn) -> Result<(), Error> {
        // self.db.compact();
        Ok(())
    }

    pub fn iter_raw_keys(&self, column: DBColumn, prefix: &[u8]) -> Result<RawKeyIter, Error> {
        println!("iter_raw_keys");
        let table_definition: TableDefinition<'_, &[u8], &[u8]> =
        TableDefinition::new(column.into());
        let tx = self.db.begin_read()?;
        let table = tx.open_table(table_definition)?;

        let result = table
            .iter()?
            .take_while( move |result| {
                if let Ok((key_guard, _)) = result {
                    let key = key_guard.value().to_vec();
                    // TODO ensure were correctly filtering by prefix
                    BytesKey::from_vec(key).starts_with(&BytesKey::from_vec(prefix.to_vec()))
                } else {
                    false
                }
            })
            .filter_map(
                |result| {
                    result.ok()
                    .map_or_else(
                        || None, // Skip if it's an error
                        |(key_guard, _)| Some(Ok(key_guard.value().to_vec()))
                    )
                
                }
            ).collect::<Vec<_>>();

        Ok(Box::new(result.into_iter()))
    }

    /// Iterate through all keys and values in a particular column.
    pub fn iter_column_keys<K: Key>(&self, column: DBColumn) -> Result<ColumnKeyIter<K>, Error> {
        println!("iter_column_keys");
        let table_definition: TableDefinition<'_, &[u8], &[u8]> =
            TableDefinition::new(column.into());
        let tx = self.db.begin_read()?;
        let table = tx.open_table(table_definition)?;

        let result = table
            .iter()?
            .filter_map( |result| {
                result.ok()
                .map_or_else(
                    || None,
                        |(key_guard, _)| {
                            Some(K::from_bytes(&key_guard.value().to_vec()))
                        })
            }).collect::<Vec<_>>();

        Ok(Box::new(result.into_iter()))
    }
}
