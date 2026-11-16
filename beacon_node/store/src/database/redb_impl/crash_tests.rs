//! Crash tests: run random writes across a hot, cold and blobs database, simulate a crash at a
//! random disk sync, and check that the reopened databases are consistent.

use super::*;
use proptest::prelude::*;
use rand::{Rng, SeedableRng, rngs::SmallRng};
use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::SeqCst;
use tempfile::tempdir;

type Contents = BTreeMap<(&'static str, Vec<u8>), Vec<u8>>;

const HOT: usize = 0;
const COLD: usize = 1;
const BLOBS: usize = 2;

const BLOCK_COL: DBColumn = DBColumn::BeaconBlock;
const BLOB_COL: DBColumn = DBColumn::BeaconBlob;
const SPLIT_COL: DBColumn = DBColumn::BeaconMeta;
const COLD_COL: DBColumn = DBColumn::BeaconBlockRoots;
const FUZZ_COL: DBColumn = DBColumn::BeaconStateSummary;
const COLUMNS: [DBColumn; 5] = [BLOCK_COL, BLOB_COL, SPLIT_COL, COLD_COL, FUZZ_COL];

/// Decides which disk sync the crash happens at. Syncs from then on are lost.
#[derive(Debug, Default)]
struct Crash {
    armed: AtomicBool,
    syncs_left: AtomicI64,
    crashed: AtomicBool,
}

impl Crash {
    fn sync_lost(&self) -> bool {
        if self.crashed.load(SeqCst) {
            return true;
        }
        if self.armed.load(SeqCst) && self.syncs_left.fetch_sub(1, SeqCst) <= 0 {
            self.crashed.store(true, SeqCst);
            return true;
        }
        false
    }
}

#[derive(Debug)]
enum Write {
    Data(u64, Vec<u8>),
    Len(u64),
}

impl Write {
    fn apply(&self, buf: &mut Vec<u8>) {
        match self {
            Write::Data(offset, data) => {
                let start = *offset as usize;
                let end = start + data.len();
                if buf.len() < end {
                    buf.resize(end, 0);
                }
                buf[start..end].copy_from_slice(data);
            }
            Write::Len(len) => buf.resize(*len as usize, 0),
        }
    }
}

#[derive(Debug, Default)]
struct File {
    live: Vec<u8>,
    durable: Vec<u8>,
    unsynced: Vec<Write>,
}

impl File {
    /// After a power loss, only some unsynced writes may have reached the disk.
    fn crash_image(&self, power_loss: bool, rng: &mut SmallRng) -> Vec<u8> {
        let mut image = self.durable.clone();
        for write in &self.unsynced {
            if !power_loss || rng.random_bool(0.5) {
                write.apply(&mut image);
            }
        }
        image
    }
}

#[derive(Debug)]
struct CrashBackend {
    crash: Arc<Crash>,
    file: Arc<Mutex<File>>,
}

impl CrashBackend {
    fn record(&self, write: Write) {
        let mut file = self.file.lock();
        write.apply(&mut file.live);
        if !self.crash.crashed.load(SeqCst) {
            file.unsynced.push(write);
        }
    }
}

impl redb::StorageBackend for CrashBackend {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.lock().live.len() as u64)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        let file = self.file.lock();
        let start = offset as usize;
        let data = file
            .live
            .get(start..start + out.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "read past end"))?;
        out.copy_from_slice(data);
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.record(Write::Len(len));
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        let mut file = self.file.lock();
        if !self.crash.sync_lost() {
            let File {
                durable, unsynced, ..
            } = &mut *file;
            for write in unsynced.drain(..) {
                write.apply(durable);
            }
        }
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.record(Write::Data(offset, data.to_vec()));
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum Op {
    ImportBlock,
    Migrate,
    Write {
        db: usize,
        kind: u8,
        key: u64,
        len: usize,
    },
    Flush,
    Compact(usize),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => Just(Op::ImportBlock),
        1 => Just(Op::Migrate),
        4 => (0..3usize, 0..5u8, 0..8u64, 0..2048usize)
            .prop_map(|(db, kind, key, len)| Op::Write { db, kind, key, len }),
        2 => Just(Op::Flush),
        1 => (0..3usize).prop_map(Op::Compact),
    ]
}

fn key(column: DBColumn, n: u64) -> Vec<u8> {
    let mut key = vec![0; column.key_size()];
    let len = key.len();
    key[len - 8..].copy_from_slice(&n.to_be_bytes());
    key
}

fn value(op_index: usize, len: usize) -> Vec<u8> {
    (0..len).map(|i| (op_index * 31 + i) as u8).collect()
}

fn delete_predicate(value: &[u8]) -> bool {
    value.first().is_some_and(|byte| byte % 2 == 0)
}

fn contents(db: &Redb) -> Contents {
    let mut contents = Contents::new();
    for column in COLUMNS {
        for result in db.iter_column::<Vec<u8>>(column) {
            let (key, value) = result.unwrap();
            contents.insert((column.into(), key), value);
        }
    }
    contents
}

struct Harness {
    dbs: Vec<Redb>,
    models: Vec<Contents>,
    snapshots: Vec<Vec<Contents>>,
    blocks: u64,
    split: u64,
}

impl Harness {
    fn put(&mut self, db: usize, column: DBColumn, key: Vec<u8>, value: Vec<u8>) {
        self.dbs[db]
            .do_atomically(vec![KeyValueStoreOp::PutKeyValue(
                column,
                key.clone(),
                value.clone(),
            )])
            .unwrap();
        self.models[db].insert((column.into(), key), value);
    }

    fn apply(&mut self, op_index: usize, op: &Op) {
        match *op {
            Op::ImportBlock => {
                let n = self.blocks;
                self.blocks += 1;
                self.put(BLOBS, BLOB_COL, key(BLOB_COL, n), value(op_index, 512));
                self.put(HOT, BLOCK_COL, key(BLOCK_COL, n), value(op_index, 256));
            }
            Op::Migrate => {
                let new_split = self.blocks;
                let ops = (self.split..new_split)
                    .map(|n| {
                        let (key, value) = (key(COLD_COL, n), n.to_be_bytes().to_vec());
                        self.models[COLD].insert((COLD_COL.into(), key.clone()), value.clone());
                        KeyValueStoreOp::PutKeyValue(COLD_COL, key, value)
                    })
                    .collect::<Vec<_>>();
                self.dbs[COLD].do_atomically(ops).unwrap();
                self.dbs[COLD].sync().unwrap();

                let (key, value) = (key(SPLIT_COL, 0), new_split.to_be_bytes().to_vec());
                self.dbs[HOT]
                    .put_bytes_sync(SPLIT_COL, &key, &value)
                    .unwrap();
                self.models[HOT].insert((SPLIT_COL.into(), key), value);
                self.split = new_split;
            }
            Op::Write {
                db,
                kind,
                key: n,
                len,
            } => {
                let model = &mut self.models[db];
                let name: &'static str = FUZZ_COL.into();
                let (key, next_key) = (key(FUZZ_COL, n), key(FUZZ_COL, n + 1));
                let value = value(op_index, len);
                let redb = &self.dbs[db];
                match kind {
                    0 => {
                        redb.put_bytes(FUZZ_COL, &key, &value).unwrap();
                        model.insert((name, key), value);
                    }
                    1 => {
                        redb.key_delete(FUZZ_COL, &key).unwrap();
                        model.remove(&(name, key));
                    }
                    2 => {
                        redb.do_atomically(vec![
                            KeyValueStoreOp::PutKeyValue(FUZZ_COL, key.clone(), value.clone()),
                            KeyValueStoreOp::DeleteKey(FUZZ_COL, next_key.clone()),
                        ])
                        .unwrap();
                        model.insert((name, key), value);
                        model.remove(&(name, next_key));
                    }
                    3 => {
                        redb.delete_batch(FUZZ_COL, [key.as_slice(), next_key.as_slice()].into())
                            .unwrap();
                        model.remove(&(name, key));
                        model.remove(&(name, next_key));
                    }
                    _ => {
                        redb.delete_if(FUZZ_COL, |value| Ok(delete_predicate(value)))
                            .unwrap();
                        model.retain(|(col, _), value| *col != name || !delete_predicate(value));
                    }
                }
            }
            Op::Flush => flush().unwrap(),
            Op::Compact(db) => self.dbs[db].compact().unwrap(),
        }
        for (snapshots, model) in self.snapshots.iter_mut().zip(&self.models) {
            snapshots.push(model.clone());
        }
    }
}

fn run(ops: Vec<Op>, crash_at: i64, power_loss: bool, seed: u64) {
    let _test_guard = TEST_LOCK.lock();
    let crash = Arc::new(Crash::default());
    let files: Vec<Arc<Mutex<File>>> = (0..3).map(|_| Default::default()).collect();

    // Opened in the order `HotColdDB` opens them, which `flush` relies on.
    let dbs = files
        .iter()
        .map(|file| {
            let backend = CrashBackend {
                crash: crash.clone(),
                file: file.clone(),
            };
            Redb::from_database(redb::Builder::new().create_with_backend(backend).unwrap()).unwrap()
        })
        .collect();
    let mut harness = Harness {
        dbs,
        models: vec![Contents::new(); 3],
        snapshots: vec![vec![Contents::new()]; 3],
        blocks: 0,
        split: 0,
    };

    crash.syncs_left.store(crash_at, SeqCst);
    crash.armed.store(true, SeqCst);
    let mut durable_index = 0;
    // redb compaction doesn't save the allocator state: https://github.com/cberner/redb/issues/829
    let mut full_repair_allowed = false;
    for (i, op) in ops.iter().enumerate() {
        harness.apply(i, op);
        if crash.crashed.load(SeqCst) {
            full_repair_allowed = matches!(op, Op::Compact(_));
            break;
        }
        if matches!(op, Op::Migrate | Op::Flush | Op::Compact(_)) {
            durable_index = i + 1;
        }
    }
    crash.crashed.store(true, SeqCst);
    let Harness { dbs, snapshots, .. } = harness;
    drop(dbs);

    let mut rng = SmallRng::seed_from_u64(seed);
    let recovered = files
        .iter()
        .map(|file| {
            let dir = tempdir().unwrap();
            let path = dir.path().join(DB_FILE_NAME);
            std::fs::write(&path, file.lock().crash_image(power_loss, &mut rng)).unwrap();
            if !full_repair_allowed {
                redb::Builder::new()
                    .set_repair_callback(|session| session.abort())
                    .create(&path)
                    .expect("full repair should not be required");
            }
            contents(&Redb::open(dir.path()).unwrap())
        })
        .collect::<Vec<_>>();

    let recovered_at = (0..3)
        .map(|db| {
            let indices = snapshots[db]
                .iter()
                .enumerate()
                .filter(|(_, snapshot)| **snapshot == recovered[db])
                .map(|(i, _)| i)
                .collect::<Vec<_>>();
            assert!(
                indices.iter().any(|&i| i >= durable_index),
                "db {db} recovered to snapshots {indices:?}, but op {durable_index} was durable"
            );
            indices
        })
        .collect::<Vec<_>>();

    for db in [COLD, BLOBS] {
        assert!(
            recovered_at[HOT].iter().min() <= recovered_at[db].iter().max(),
            "hot recovered to {:?}, ahead of db {db} at {:?}",
            recovered_at[HOT],
            recovered_at[db]
        );
    }

    let block_col: &'static str = BLOCK_COL.into();
    for (col, key) in recovered[HOT].keys() {
        if *col == block_col {
            assert!(
                recovered[BLOBS].contains_key(&(BLOB_COL.into(), key.clone())),
                "block {key:?} is missing its blob"
            );
        }
    }

    if let Some(split) = recovered[HOT].get(&(SPLIT_COL.into(), key(SPLIT_COL, 0))) {
        let split = u64::from_be_bytes(split.as_slice().try_into().unwrap());
        for n in 0..split {
            assert!(
                recovered[COLD].contains_key(&(COLD_COL.into(), key(COLD_COL, n))),
                "slot {n} is before the split {split} but not in the cold database"
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    #[test]
    fn crash_recovery(
        ops in prop::collection::vec(op(), 1..40),
        crash_at in 0..80i64,
        power_loss: bool,
        seed: u64,
    ) {
        run(ops, crash_at, power_loss, seed);
    }
}
