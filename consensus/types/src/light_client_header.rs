use crate::light_client_update::*;
use crate::{
    test_utils::TestRandom, EthSpec, ExecutionPayloadHeader, FixedVector, Hash256,
    SignedBeaconBlock,
};
use crate::{BeaconBlockHeader, ExecutionPayload};
use merkle_proof::{verify_merkle_proof, MerkleTree};
use serde::{Deserialize, Serialize};
use ssz::Encode;
use ssz_derive::{Decode, Encode};
use test_random_derive::TestRandom;
use tree_hash::TreeHash;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    Encode,
    Decode,
    TestRandom,
    arbitrary::Arbitrary,
)]
#[serde(bound = "E: EthSpec")]
#[arbitrary(bound = "E: EthSpec")]
pub struct LightClientHeader<E: EthSpec> {
    pub beacon: BeaconBlockHeader,
    #[test_random(default)]
    #[ssz(skip_serializing, skip_deserializing)]
    pub execution: Option<ExecutionPayloadHeader<E>>,
    #[test_random(default)]
    pub execution_branch: Option<FixedVector<Hash256, ExecutionPayloadProofLen>>,
}

impl<E: EthSpec> From<BeaconBlockHeader> for LightClientHeader<E> {
    fn from(beacon: BeaconBlockHeader) -> Self {
        LightClientHeader {
            beacon,
            execution: None,
            execution_branch: None,
        }
    }
}

impl<E: EthSpec> LightClientHeader<E> {
    fn new(block: SignedBeaconBlock<E>) -> Result<Self, Error> {
        let epoch = block.message().slot().epoch(E::slots_per_epoch());

        // TODO epoch greater than or equal to capella
        let payload: ExecutionPayload<E> = if epoch >= 0 {
            block
                .message()
                .execution_payload()?
                .execution_payload_capella()?
                .to_owned()
                .into()
        } else if epoch >= 1 {
            block
                .message()
                .execution_payload()?
                .execution_payload_deneb()?
                .to_owned()
                .into()
        } else {
            return Ok(LightClientHeader {
                beacon: block.message().block_header(),
                execution: None,
                execution_branch: None,
            });
        };

        // TODO fix unwrap
        let header = ExecutionPayloadHeader::from(payload.to_ref());
        let leaves = block
            .message()
            .body_capella()?
            .as_ssz_bytes()
            .iter()
            .map(|data| data.tree_hash_root())
            .collect::<Vec<_>>();

        let tree = MerkleTree::create(&leaves, EXECUTION_PAYLOAD_PROOF_LEN);

        let (_, proof) =
            tree.generate_proof(EXECUTION_PAYLOAD_INDEX, EXECUTION_PAYLOAD_PROOF_LEN)?;

        Ok(LightClientHeader {
            beacon: block.message().block_header(),
            execution: Some(header),
            execution_branch: Some(proof.into()),
        })
    }

    fn get_lc_execution_root(&self) -> Option<Hash256> {
        let epoch = self.beacon.slot.epoch(E::slots_per_epoch());

        // TODO greater than or equal to CAPELLA
        if epoch >= 0 {
            if let Some(execution) = &self.execution {
                return Some(execution.tree_hash_root());
            }
        }

        None
    }

    fn is_valid_light_client_header(&self) -> Result<bool, Error> {
        let epoch = self.beacon.slot.epoch(E::slots_per_epoch());

        // TODO LESS THAN DENEB
        if epoch < 1 {
            let Some(execution) = &self.execution else {
                return Ok(false);
            };

            // TODO unwrap
            if execution.blob_gas_used()?.to_owned() != 0 as u64
                || execution.excess_blob_gas()?.to_owned() != 0 as u64
            {
                return Ok(false);
            }
        }

        // TODO LESS THAN DENEB
        if epoch < 0 {
            return Ok(self.execution == None && self.execution_branch == None);
        }

        let Some(execution_root) = self.get_lc_execution_root() else {
            return Ok(false);
        };

        let Some(execution_branch) = &self.execution_branch else {
            return Ok(false);
        };

        Ok(verify_merkle_proof(
            execution_root,
            &execution_branch,
            EXECUTION_PAYLOAD_PROOF_LEN,
            get_subtree_index(EXECUTION_PAYLOAD_INDEX as u32) as usize,
            self.beacon.body_root,
        ))
    }
}

// TODO move to the relevant place
fn get_subtree_index(generalized_index: u32) -> u32 {
    return generalized_index % 2 * (log2_int(generalized_index));
}

// TODO move to the relevant place
fn log2_int(x: u32) -> u32 {
    if x == 0 {
        return 0;
    }
    31 - x.leading_zeros()
}
