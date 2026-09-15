use std::marker::PhantomData;

use context_deserialize::{ContextDeserialize, context_deserialize};
use educe::Educe;
use serde::{Deserialize, Deserializer, Serialize};
use ssz::Decode;
use ssz_derive::{Decode, Encode};
use ssz_types::FixedVector;
use superstruct::superstruct;
use tree_hash_derive::TreeHash;

use crate::{
    block::{BeaconBlockHeader, SignedBlindedBeaconBlock},
    core::{ChainSpec, EthSpec, Hash256},
    execution::{
        ExecPayload, ExecutionPayloadHeader, ExecutionPayloadHeaderCapella,
        ExecutionPayloadHeaderDeneb, ExecutionPayloadHeaderElectra, ExecutionPayloadHeaderFulu,
    },
    fork::ForkName,
    light_client::{ExecutionPayloadProofLen, LightClientError, consts::EXECUTION_PAYLOAD_INDEX},
};

#[superstruct(
    variants(Altair, Capella, Deneb, Electra, Fulu,),
    variant_attributes(
        derive(Debug, Clone, Serialize, Deserialize, Educe, Decode, Encode, TreeHash,),
        educe(PartialEq),
        serde(bound = "E: EthSpec", deny_unknown_fields),
        cfg_attr(
            feature = "arbitrary",
            derive(arbitrary::Arbitrary),
            arbitrary(bound = "E: EthSpec"),
        ),
        context_deserialize(ForkName),
    )
)]
#[cfg_attr(
    feature = "arbitrary",
    derive(arbitrary::Arbitrary),
    arbitrary(bound = "E: EthSpec")
)]
#[derive(Debug, Clone, Serialize, TreeHash, Encode, PartialEq)]
#[serde(untagged)]
#[tree_hash(enum_behaviour = "transparent")]
#[ssz(enum_behaviour = "transparent")]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
pub struct LightClientHeader<E: EthSpec> {
    pub beacon: BeaconBlockHeader,

    #[superstruct(
        only(Capella),
        partial_getter(rename = "execution_payload_header_capella")
    )]
    pub execution: ExecutionPayloadHeaderCapella<E>,
    #[superstruct(only(Deneb), partial_getter(rename = "execution_payload_header_deneb"))]
    pub execution: ExecutionPayloadHeaderDeneb<E>,
    #[superstruct(
        only(Electra),
        partial_getter(rename = "execution_payload_header_electra")
    )]
    pub execution: ExecutionPayloadHeaderElectra<E>,
    #[superstruct(only(Fulu), partial_getter(rename = "execution_payload_header_fulu"))]
    pub execution: ExecutionPayloadHeaderFulu<E>,

    #[superstruct(only(Capella, Deneb, Electra, Fulu))]
    pub execution_branch: FixedVector<Hash256, ExecutionPayloadProofLen>,

    #[ssz(skip_serializing, skip_deserializing)]
    #[tree_hash(skip_hashing)]
    #[serde(skip)]
    #[cfg_attr(feature = "arbitrary", arbitrary(default))]
    pub _phantom_data: PhantomData<E>,
}

impl<E: EthSpec> LightClientHeader<E> {
    pub fn block_to_light_client_header(
        block: &SignedBlindedBeaconBlock<E>,
        chain_spec: &ChainSpec,
    ) -> Result<Self, LightClientError> {
        let header = match block
            .fork_name(chain_spec)
            .map_err(|_| LightClientError::InconsistentFork)?
        {
            ForkName::Base => return Err(LightClientError::AltairForkNotActive),
            ForkName::Altair | ForkName::Bellatrix => LightClientHeader::Altair(
                LightClientHeaderAltair::block_to_light_client_header(block)?,
            ),
            ForkName::Capella => LightClientHeader::Capella(
                LightClientHeaderCapella::block_to_light_client_header(block)?,
            ),
            ForkName::Deneb => LightClientHeader::Deneb(
                LightClientHeaderDeneb::block_to_light_client_header(block)?,
            ),
            ForkName::Electra => LightClientHeader::Electra(
                LightClientHeaderElectra::block_to_light_client_header(block)?,
            ),
            ForkName::Fulu => {
                LightClientHeader::Fulu(LightClientHeaderFulu::block_to_light_client_header(block)?)
            }
            // TODO(gloas): implement Gloas light client
            ForkName::Gloas => return Err(LightClientError::GloasNotImplemented),
            ForkName::Heze => return Err(LightClientError::HezeNotImplemented),
        };
        Ok(header)
    }

    pub fn from_ssz_bytes(bytes: &[u8], fork_name: ForkName) -> Result<Self, ssz::DecodeError> {
        let header = match fork_name {
            ForkName::Altair | ForkName::Bellatrix => {
                LightClientHeader::Altair(LightClientHeaderAltair::from_ssz_bytes(bytes)?)
            }
            ForkName::Capella => {
                LightClientHeader::Capella(LightClientHeaderCapella::from_ssz_bytes(bytes)?)
            }
            ForkName::Deneb => {
                LightClientHeader::Deneb(LightClientHeaderDeneb::from_ssz_bytes(bytes)?)
            }
            ForkName::Electra => {
                LightClientHeader::Electra(LightClientHeaderElectra::from_ssz_bytes(bytes)?)
            }
            ForkName::Fulu => {
                LightClientHeader::Fulu(LightClientHeaderFulu::from_ssz_bytes(bytes)?)
            }
            // TODO(gloas): implement Gloas light client
            ForkName::Base | ForkName::Gloas | ForkName::Heze => {
                return Err(ssz::DecodeError::BytesInvalid(format!(
                    "LightClientHeader decoding for {fork_name} not implemented"
                )));
            }
        };

        Ok(header)
    }

    /// Custom SSZ decoder that takes a `ForkName` as context.
    pub fn from_ssz_bytes_for_fork(
        bytes: &[u8],
        fork_name: ForkName,
    ) -> Result<Self, ssz::DecodeError> {
        Self::from_ssz_bytes(bytes, fork_name)
    }

    pub fn ssz_max_var_len_for_fork(fork_name: ForkName) -> usize {
        if fork_name.gloas_enabled() {
            // TODO(EIP7732): check this
            0
        } else if fork_name.capella_enabled() {
            ExecutionPayloadHeader::<E>::ssz_max_var_len_for_fork(fork_name)
        } else {
            0
        }
    }
}

type ExecutionHeaderAndBranch<E> = Option<(ExecutionPayloadHeader<E>, Vec<Hash256>)>;

/// Returns the execution payload header of `block` and the branch proving it in the block body.
///
/// Returns `None` for blocks prior to Capella, which carry no execution data in light client
/// headers. The block may belong to an earlier fork than the header being built: during fork
/// transitions the finalized block lags the attested block.
fn execution_header_and_branch<E: EthSpec>(
    block: &SignedBlindedBeaconBlock<E>,
) -> Result<ExecutionHeaderAndBranch<E>, LightClientError> {
    if !block.fork_name_unchecked().capella_enabled() {
        return Ok(None);
    }
    let header = block
        .message()
        .execution_payload()?
        .to_execution_payload_header();
    let branch = block
        .message()
        .body()
        .block_body_merkle_proof(EXECUTION_PAYLOAD_INDEX)?;
    Ok(Some((header, branch)))
}

impl<E: EthSpec> LightClientHeaderAltair<E> {
    pub fn block_to_light_client_header(
        block: &SignedBlindedBeaconBlock<E>,
    ) -> Result<Self, LightClientError> {
        Ok(LightClientHeaderAltair {
            beacon: block.message().block_header(),
            _phantom_data: PhantomData,
        })
    }
}

impl<E: EthSpec> Default for LightClientHeaderAltair<E> {
    fn default() -> Self {
        Self {
            beacon: BeaconBlockHeader::empty(),
            _phantom_data: PhantomData,
        }
    }
}

impl<E: EthSpec> LightClientHeaderCapella<E> {
    pub fn block_to_light_client_header(
        block: &SignedBlindedBeaconBlock<E>,
    ) -> Result<Self, LightClientError> {
        let (execution, execution_branch) = match execution_header_and_branch(block)? {
            Some((header, branch)) => {
                let ExecutionPayloadHeader::Capella(execution) = header else {
                    return Err(LightClientError::InconsistentFork);
                };
                (execution, FixedVector::new(branch)?)
            }
            None => (
                ExecutionPayloadHeaderCapella::default(),
                FixedVector::default(),
            ),
        };

        Ok(LightClientHeaderCapella {
            beacon: block.message().block_header(),
            execution,
            execution_branch,
            _phantom_data: PhantomData,
        })
    }
}

impl<E: EthSpec> Default for LightClientHeaderCapella<E> {
    fn default() -> Self {
        Self {
            beacon: BeaconBlockHeader::empty(),
            execution: ExecutionPayloadHeaderCapella::default(),
            execution_branch: FixedVector::default(),
            _phantom_data: PhantomData,
        }
    }
}

impl<E: EthSpec> LightClientHeaderDeneb<E> {
    pub fn block_to_light_client_header(
        block: &SignedBlindedBeaconBlock<E>,
    ) -> Result<Self, LightClientError> {
        let (execution, execution_branch) = match execution_header_and_branch(block)? {
            Some((header, branch)) => {
                let execution = match header {
                    ExecutionPayloadHeader::Capella(header) => header.upgrade_to_deneb(),
                    ExecutionPayloadHeader::Deneb(header) => header,
                    _ => return Err(LightClientError::InconsistentFork),
                };
                (execution, FixedVector::new(branch)?)
            }
            None => (
                ExecutionPayloadHeaderDeneb::default(),
                FixedVector::default(),
            ),
        };

        Ok(LightClientHeaderDeneb {
            beacon: block.message().block_header(),
            execution,
            execution_branch,
            _phantom_data: PhantomData,
        })
    }
}

impl<E: EthSpec> Default for LightClientHeaderDeneb<E> {
    fn default() -> Self {
        Self {
            beacon: BeaconBlockHeader::empty(),
            execution: ExecutionPayloadHeaderDeneb::default(),
            execution_branch: FixedVector::default(),
            _phantom_data: PhantomData,
        }
    }
}

impl<E: EthSpec> LightClientHeaderElectra<E> {
    pub fn block_to_light_client_header(
        block: &SignedBlindedBeaconBlock<E>,
    ) -> Result<Self, LightClientError> {
        let (execution, execution_branch) = match execution_header_and_branch(block)? {
            Some((header, branch)) => {
                let execution = match header {
                    ExecutionPayloadHeader::Capella(header) => {
                        header.upgrade_to_deneb().upgrade_to_electra()
                    }
                    ExecutionPayloadHeader::Deneb(header) => header.upgrade_to_electra(),
                    ExecutionPayloadHeader::Electra(header) => header,
                    _ => return Err(LightClientError::InconsistentFork),
                };
                (execution, FixedVector::new(branch)?)
            }
            None => (
                ExecutionPayloadHeaderElectra::default(),
                FixedVector::default(),
            ),
        };

        Ok(LightClientHeaderElectra {
            beacon: block.message().block_header(),
            execution,
            execution_branch,
            _phantom_data: PhantomData,
        })
    }
}

impl<E: EthSpec> Default for LightClientHeaderElectra<E> {
    fn default() -> Self {
        Self {
            beacon: BeaconBlockHeader::empty(),
            execution: ExecutionPayloadHeaderElectra::default(),
            execution_branch: FixedVector::default(),
            _phantom_data: PhantomData,
        }
    }
}

impl<E: EthSpec> LightClientHeaderFulu<E> {
    pub fn block_to_light_client_header(
        block: &SignedBlindedBeaconBlock<E>,
    ) -> Result<Self, LightClientError> {
        let (execution, execution_branch) = match execution_header_and_branch(block)? {
            Some((header, branch)) => {
                let execution = match header {
                    ExecutionPayloadHeader::Capella(header) => header
                        .upgrade_to_deneb()
                        .upgrade_to_electra()
                        .upgrade_to_fulu(),
                    ExecutionPayloadHeader::Deneb(header) => {
                        header.upgrade_to_electra().upgrade_to_fulu()
                    }
                    ExecutionPayloadHeader::Electra(header) => header.upgrade_to_fulu(),
                    ExecutionPayloadHeader::Fulu(header) => header,
                    _ => return Err(LightClientError::InconsistentFork),
                };
                (execution, FixedVector::new(branch)?)
            }
            None => (
                ExecutionPayloadHeaderFulu::default(),
                FixedVector::default(),
            ),
        };

        Ok(LightClientHeaderFulu {
            beacon: block.message().block_header(),
            execution,
            execution_branch,
            _phantom_data: PhantomData,
        })
    }
}

impl<E: EthSpec> Default for LightClientHeaderFulu<E> {
    fn default() -> Self {
        Self {
            beacon: BeaconBlockHeader::empty(),
            execution: ExecutionPayloadHeaderFulu::default(),
            execution_branch: FixedVector::default(),
            _phantom_data: PhantomData,
        }
    }
}

impl<'de, E: EthSpec> ContextDeserialize<'de, ForkName> for LightClientHeader<E> {
    fn context_deserialize<D>(deserializer: D, context: ForkName) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let convert_err = |e| {
            serde::de::Error::custom(format!(
                "LightClientFinalityUpdate failed to deserialize: {:?}",
                e
            ))
        };
        Ok(match context {
            // TODO(gloas): implement Gloas light client
            ForkName::Base | ForkName::Gloas | ForkName::Heze => {
                return Err(serde::de::Error::custom(format!(
                    "LightClientFinalityUpdate failed to deserialize: unsupported fork '{}'",
                    context
                )));
            }
            ForkName::Altair | ForkName::Bellatrix => {
                Self::Altair(Deserialize::deserialize(deserializer).map_err(convert_err)?)
            }
            ForkName::Capella => {
                Self::Capella(Deserialize::deserialize(deserializer).map_err(convert_err)?)
            }
            ForkName::Deneb => {
                Self::Deneb(Deserialize::deserialize(deserializer).map_err(convert_err)?)
            }
            ForkName::Electra => {
                Self::Electra(Deserialize::deserialize(deserializer).map_err(convert_err)?)
            }
            ForkName::Fulu => {
                Self::Fulu(Deserialize::deserialize(deserializer).map_err(convert_err)?)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    // `ssz_tests!` can only be defined once per namespace
    #[cfg(test)]
    mod altair {
        use crate::{LightClientHeaderAltair, MainnetEthSpec};
        ssz_tests!(LightClientHeaderAltair<MainnetEthSpec>);
    }

    #[cfg(test)]
    mod capella {
        use crate::{LightClientHeaderCapella, MainnetEthSpec};
        ssz_tests!(LightClientHeaderCapella<MainnetEthSpec>);
    }

    #[cfg(test)]
    mod deneb {
        use crate::{LightClientHeaderDeneb, MainnetEthSpec};
        ssz_tests!(LightClientHeaderDeneb<MainnetEthSpec>);
    }

    #[cfg(test)]
    mod electra {
        use crate::{LightClientHeaderElectra, MainnetEthSpec};
        ssz_tests!(LightClientHeaderElectra<MainnetEthSpec>);
    }

    #[cfg(test)]
    mod fulu {
        use crate::{LightClientHeaderFulu, MainnetEthSpec};
        ssz_tests!(LightClientHeaderFulu<MainnetEthSpec>);
    }
}

#[cfg(test)]
mod fork_transition_tests {
    use super::*;
    use crate::{
        BlindedPayload, MainnetEthSpec, SignedBeaconBlockBellatrix, SignedBeaconBlockCapella,
        SignedBeaconBlockElectra, light_client::consts::EXECUTION_PAYLOAD_PROOF_LEN,
        test_utils::test_arbitrary_instance,
    };
    use merkle_proof::verify_merkle_proof;
    use tree_hash::TreeHash;

    type E = MainnetEthSpec;

    fn assert_execution_branch_valid(
        execution_root: Hash256,
        branch: &[Hash256],
        block: &SignedBlindedBeaconBlock<E>,
    ) {
        assert!(verify_merkle_proof(
            execution_root,
            branch,
            EXECUTION_PAYLOAD_PROOF_LEN,
            EXECUTION_PAYLOAD_INDEX - (1 << EXECUTION_PAYLOAD_PROOF_LEN),
            block.message().body_root(),
        ));
    }

    #[test]
    fn fulu_header_from_electra_block() {
        let block = SignedBlindedBeaconBlock::Electra(test_arbitrary_instance::<
            SignedBeaconBlockElectra<E, BlindedPayload<E>>,
        >());
        let payload_header = block
            .message()
            .execution_payload()
            .unwrap()
            .execution_payload_electra()
            .unwrap()
            .clone();

        let header = LightClientHeaderFulu::block_to_light_client_header(&block).unwrap();

        assert_eq!(header.beacon, block.message().block_header());
        assert_eq!(header.execution, payload_header.upgrade_to_fulu());
        assert_execution_branch_valid(
            payload_header.tree_hash_root(),
            &header.execution_branch,
            &block,
        );
    }

    #[test]
    fn deneb_header_from_capella_block() {
        let block = SignedBlindedBeaconBlock::Capella(test_arbitrary_instance::<
            SignedBeaconBlockCapella<E, BlindedPayload<E>>,
        >());
        let payload_header = block
            .message()
            .execution_payload()
            .unwrap()
            .execution_payload_capella()
            .unwrap()
            .clone();

        let header = LightClientHeaderDeneb::block_to_light_client_header(&block).unwrap();

        assert_eq!(header.beacon, block.message().block_header());
        assert_eq!(header.execution, payload_header.upgrade_to_deneb());
        assert_eq!(header.execution.blob_gas_used, 0);
        assert_eq!(header.execution.excess_blob_gas, 0);
        assert_execution_branch_valid(
            payload_header.tree_hash_root(),
            &header.execution_branch,
            &block,
        );
    }

    #[test]
    fn capella_header_from_bellatrix_block() {
        let block = SignedBlindedBeaconBlock::Bellatrix(test_arbitrary_instance::<
            SignedBeaconBlockBellatrix<E, BlindedPayload<E>>,
        >());

        let header = LightClientHeaderCapella::block_to_light_client_header(&block).unwrap();

        assert_eq!(header.beacon, block.message().block_header());
        assert_eq!(header.execution, ExecutionPayloadHeaderCapella::default());
        assert_eq!(header.execution_branch, FixedVector::default());
    }

    #[test]
    fn capella_header_from_deneb_block_is_rejected() {
        let block = SignedBlindedBeaconBlock::Deneb(test_arbitrary_instance::<
            crate::SignedBeaconBlockDeneb<E, BlindedPayload<E>>,
        >());

        assert_eq!(
            LightClientHeaderCapella::block_to_light_client_header(&block).unwrap_err(),
            LightClientError::InconsistentFork
        );
    }
}
