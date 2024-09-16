use crate::chunked_vector::ChunkError;
use crate::config::StoreConfigError;
use crate::hot_cold_store::HotColdDBError;
#[cfg(feature = "leveldb")]
use leveldb::error::Error as LevelDBError;
use ssz::DecodeError;
use state_processing::BlockReplayError;
use types::{BeaconStateError, EpochCacheError, Hash256, InconsistentFork, Slot};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    SszDecodeError(DecodeError),
    VectorChunkError(ChunkError),
    BeaconStateError(BeaconStateError),
    PartialBeaconStateError,
    HotColdDBError(HotColdDBError),
    DBError {
        message: String,
    },
    RlpError(String),
    BlockNotFound(Hash256),
    NoContinuationData,
    SplitPointModified(Slot, Slot),
    ConfigError(StoreConfigError),
    SchemaMigrationError(String),
    /// The store's `anchor_info` was mutated concurrently, the latest modification wasn't applied.
    AnchorInfoConcurrentMutation,
    /// The store's `blob_info` was mutated concurrently, the latest modification wasn't applied.
    BlobInfoConcurrentMutation,
    /// The store's `data_column_info` was mutated concurrently, the latest modification wasn't applied.
    DataColumnInfoConcurrentMutation,
    /// The block or state is unavailable due to weak subjectivity sync.
    HistoryUnavailable,
    /// State reconstruction cannot commence because not all historic blocks are known.
    MissingHistoricBlocks {
        oldest_block_slot: Slot,
    },
    /// State reconstruction failed because it didn't reach the upper limit slot.
    ///
    /// This should never happen (it's a logic error).
    StateReconstructionDidNotComplete,
    StateReconstructionRootMismatch {
        slot: Slot,
        expected: Hash256,
        computed: Hash256,
    },
    BlockReplayError(BlockReplayError),
    AddPayloadLogicError,
    SlotClockUnavailableForMigration,
    InvalidKey,
    InvalidBytes,
    UnableToDowngrade,
    InconsistentFork(InconsistentFork),
    #[cfg(feature = "leveldb")]
    LevelDbError(LevelDBError),
    #[cfg(feature = "redb")]
    RedbError(redb::Error),
    CacheBuildError(EpochCacheError),
    RandaoMixOutOfBounds,
    FinalizedStateDecreasingSlot,
    FinalizedStateUnaligned,
    StateForCacheHasPendingUpdates {
        state_root: Hash256,
        slot: Slot,
    },
    ArithError(safe_arith::ArithError),
}

pub trait HandleUnavailable<T> {
    fn handle_unavailable(self) -> std::result::Result<Option<T>, Error>;
}

impl<T> HandleUnavailable<T> for Result<T> {
    fn handle_unavailable(self) -> std::result::Result<Option<T>, Error> {
        match self {
            Ok(x) => Ok(Some(x)),
            Err(Error::HistoryUnavailable) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl From<DecodeError> for Error {
    fn from(e: DecodeError) -> Error {
        Error::SszDecodeError(e)
    }
}

impl From<ChunkError> for Error {
    fn from(e: ChunkError) -> Error {
        Error::VectorChunkError(e)
    }
}

impl From<HotColdDBError> for Error {
    fn from(e: HotColdDBError) -> Error {
        Error::HotColdDBError(e)
    }
}

impl From<BeaconStateError> for Error {
    fn from(e: BeaconStateError) -> Error {
        Error::BeaconStateError(e)
    }
}

impl From<DBError> for Error {
    fn from(e: DBError) -> Error {
        Error::DBError { message: e.message }
    }
}

impl From<StoreConfigError> for Error {
    fn from(e: StoreConfigError) -> Error {
        Error::ConfigError(e)
    }
}

impl From<BlockReplayError> for Error {
    fn from(e: BlockReplayError) -> Error {
        Error::BlockReplayError(e)
    }
}

impl From<InconsistentFork> for Error {
    fn from(e: InconsistentFork) -> Error {
        Error::InconsistentFork(e)
    }
}

#[cfg(feature = "leveldb")]
impl From<LevelDBError> for Error {
    fn from(e: LevelDBError) -> Error {
        Error::LevelDbError(e)
    }
}

#[cfg(feature = "redb")]
impl From<redb::Error> for Error {
    fn from(e: redb::Error) -> Self {
        Error::RedbError(e)
    }
}

#[cfg(feature = "redb")]
impl From<redb::TableError> for Error {
    fn from(e: redb::TableError) -> Self {
        Error::RedbError(e.into())
    }
}

#[cfg(feature = "redb")]
impl From<redb::TransactionError> for Error {
    fn from(e: redb::TransactionError) -> Self {
        Error::RedbError(e.into())
    }
}

#[cfg(feature = "redb")]
impl From<redb::DatabaseError> for Error {
    fn from(e: redb::DatabaseError) -> Self {
        Error::RedbError(e.into())
    }
}

#[cfg(feature = "redb")]
impl From<redb::StorageError> for Error {
    fn from(e: redb::StorageError) -> Self {
        Error::RedbError(e.into())
    }
}

#[cfg(feature = "redb")]
impl From<redb::CommitError> for Error {
    fn from(e: redb::CommitError) -> Self {
        Error::RedbError(e.into())
    }
}

#[cfg(feature = "redb")]
impl From<redb::CompactionError> for Error {
    fn from(e: redb::CompactionError) -> Self {
        Error::RedbError(e.into())
    }
}

impl From<EpochCacheError> for Error {
    fn from(e: EpochCacheError) -> Error {
        Error::CacheBuildError(e)
    }
}

impl From<safe_arith::ArithError> for Error {
    fn from(e: safe_arith::ArithError) -> Error {
        Error::ArithError(e)
    }
}

#[derive(Debug)]
pub struct DBError {
    pub message: String,
}

impl DBError {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}
