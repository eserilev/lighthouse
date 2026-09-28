use typenum::{Prod, U4096, U131072, U262144, U524288};

pub const IMMEDIATE_JUSTIFICATION_WINDOW: u64 = 5;

pub type HistoricalRootsLimit = U262144;
pub type ValidatorRegistryLimit = U4096;
pub type JustificationValidatorsLimit = Prod<HistoricalRootsLimit, ValidatorRegistryLimit>;
pub type ProofBytesLimit = U524288;

pub const XMSS_LOG_LIFETIME: usize = 32;
pub const XMSS_DIMENSION: usize = 46;
pub type XmssNodeListLimit = U131072;
