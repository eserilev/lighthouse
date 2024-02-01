use crate::{
    get_key_for_col,
    hot_cold_store::{BytesKey, HotColdDBError},
    metrics, ColumnKeyIter, Key,
};
use crate::{DBColumn, Error, KeyValueStoreOp};
use redb::{ReadableTable, TableDefinition};
use std::{f64::consts::E, marker::PhantomData, path::Path};
use strum::IntoEnumIterator;
use types::{EthSpec, Hash256};
use parking_lot::{Mutex, MutexGuard};

use super::interface::WriteOptions;

const TABLE_NAME: &str = "TABLE";

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
        let db = redb::Database::create(path)?;
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
        println!("{}", col);
        println!("{:?}", key);
        metrics::inc_counter(&metrics::DISK_DB_WRITE_COUNT);
        metrics::inc_counter_by(&metrics::DISK_DB_WRITE_BYTES, val.len() as u64);
        let timer = metrics::start_timer(&metrics::DISK_DB_WRITE_TIMES);
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
        println!("get_bytes");
        metrics::inc_counter(&metrics::DISK_DB_READ_COUNT);
        let timer = metrics::start_timer(&metrics::DISK_DB_READ_TIMES);
        let column_key = get_key_for_col(col, key);

        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(TABLE_NAME);
        let tx = self.db.begin_read()?;
        let table = tx.open_table(table_definition)?;

        let result = table.get(column_key.as_slice())?;

        // TODO: clean this up
        if let Some(access_guard) = result {
            let value = access_guard.value().to_vec();
            println!("{}", col);
            println!("{:?}", key);
            println!("get_bytes found");
            metrics::inc_counter_by(&metrics::DISK_DB_READ_BYTES, value.len() as u64);
            metrics::stop_timer(timer);
            Ok(Some(value))
        } else {
            println!("{}", col);
            println!("{:?}", key);
            println!("get_bytes not found");
            metrics::inc_counter_by(&metrics::DISK_DB_READ_BYTES, 0 as u64);
            metrics::stop_timer(timer);
            Ok(None)
        }
    }

    /// Return `true` if `key` exists in `column`.
    pub fn key_exists(&self, col: &str, key: &[u8]) -> Result<bool, Error> {
        metrics::inc_counter(&metrics::DISK_DB_EXISTS_COUNT);
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
        let column_key = get_key_for_col(col, key);
        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(TABLE_NAME);
        let tx = self.db.begin_write()?;
        let mut table = tx.open_table(table_definition)?;

        metrics::inc_counter(&metrics::DISK_DB_DELETE_COUNT);

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
                    println!("{}", column);
                    println!("{:?}", key);  
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
        Ok(()) // self.db.compact().map_err(Into::into).map(|_| ())
    }

    /// TODO resolve unwraps and clean this up
    /// Iterate through all keys and values in a particular column.
    pub fn iter_column_keys<K: Key>(&self, column: DBColumn) -> ColumnKeyIter<K> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> =
            TableDefinition::new(column.into());
        let tx = self.db.begin_read().unwrap();
        let table = tx.open_table(table_definition).unwrap();

        let x = table
            .iter()
            .unwrap()
            .take_while( |result| {
                if let Ok(access_guard) = result {
                    let key = access_guard.0.value().to_vec();
                    BytesKey::from_vec(key).matches_column(column)
                } else {
                    false
                }
            })
            .map( |result| {
                let access_guard = result.unwrap();
                let key = access_guard.0.value().to_vec();
                let bytes_key = BytesKey::from_vec(key);
                let key = bytes_key.remove_column_variable(column).ok_or_else(|| {
                    HotColdDBError::IterationError {
                        unexpected_key: bytes_key.clone(),
                    }
                })?;
                K::from_bytes(key)
            });
        Box::new(std::iter::empty())
        /*
        Box::new(
            table
                .iter()
                .unwrap()
                .take_while(move |result| {
                    let access_guard = result.unwrap();
                    if let Ok(access_guard) = result {
                        let key = access_guard.0.value().to_vec();
                        BytesKey::from_vec(key).matches_column(column)
                    } else {
                        false
                    }
                })
                .map(move |result| {
                    let access_guard = result.unwrap();
                    let key = access_guard.0.value().to_vec();
                    let bytes_key = BytesKey::from_vec(key);
                    let key = bytes_key.remove_column_variable(column).ok_or_else(|| {
                        HotColdDBError::IterationError {
                            unexpected_key: bytes_key.clone(),
                        }
                    })?;
                    K::from_bytes(key)
                }),
        )
         */
    }

    /*
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
    }*/
}
