use crate::decode_hex;
use lean_types::*;
use serde::Deserialize;
use ssz_types::typenum::Unsigned;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonList<T> {
    pub data: Vec<T>,
}

pub fn hash(value: &str) -> Result<Hash256, String> {
    Hash256::try_from(decode_hex(value)?.as_slice()).map_err(|e| format!("{value}: {e}"))
}

pub fn hashes<N: Unsigned + Clone>(values: &[String]) -> Result<VariableList<Hash256, N>, String> {
    let hashes = values
        .iter()
        .map(|value| hash(value))
        .collect::<Result<Vec<_>, _>>()?;
    VariableList::new(hashes).map_err(|e| format!("{e:?}"))
}

pub fn bitlist<N: Unsigned + Clone>(bits: &[bool]) -> Result<BitList<N>, String> {
    let mut bitlist = BitList::with_capacity(bits.len()).map_err(|e| format!("{e:?}"))?;
    for (index, bit) in bits.iter().enumerate() {
        bitlist.set(index, *bit).map_err(|e| format!("{e:?}"))?;
    }
    Ok(bitlist)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JsonCheckpoint {
    root: String,
    slot: u64,
}

impl JsonCheckpoint {
    pub fn to_checkpoint(&self) -> Result<Checkpoint, String> {
        Ok(Checkpoint {
            root: hash(&self.root)?,
            slot: Slot::new(self.slot),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JsonAttestationData {
    slot: u64,
    head: JsonCheckpoint,
    target: JsonCheckpoint,
    source: JsonCheckpoint,
}

impl JsonAttestationData {
    pub fn to_attestation_data(&self) -> Result<AttestationData, String> {
        Ok(AttestationData {
            slot: Slot::new(self.slot),
            head: self.head.to_checkpoint()?,
            target: self.target.to_checkpoint()?,
            source: self.source.to_checkpoint()?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JsonAggregatedAttestation {
    aggregation_bits: JsonList<bool>,
    data: JsonAttestationData,
}

impl JsonAggregatedAttestation {
    pub fn to_aggregated_attestation(&self) -> Result<AggregatedAttestation, String> {
        Ok(AggregatedAttestation {
            aggregation_bits: bitlist(&self.aggregation_bits.data)?,
            data: self.data.to_attestation_data()?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JsonBlockBody {
    attestations: JsonList<JsonAggregatedAttestation>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JsonBlock {
    slot: u64,
    proposer_index: u64,
    parent_root: String,
    state_root: String,
    body: JsonBlockBody,
}

impl JsonBlock {
    pub fn to_block(&self) -> Result<Block, String> {
        let attestations = self
            .body
            .attestations
            .data
            .iter()
            .map(JsonAggregatedAttestation::to_aggregated_attestation)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Block {
            slot: Slot::new(self.slot),
            proposer_index: self.proposer_index,
            parent_root: hash(&self.parent_root)?,
            state_root: hash(&self.state_root)?,
            body: BlockBody {
                attestations: VariableList::new(attestations).map_err(|e| format!("{e:?}"))?,
            },
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JsonBlockHeader {
    slot: u64,
    proposer_index: u64,
    parent_root: String,
    state_root: String,
    body_root: String,
}

impl JsonBlockHeader {
    pub fn to_block_header(&self) -> Result<BlockHeader, String> {
        Ok(BlockHeader {
            slot: Slot::new(self.slot),
            proposer_index: self.proposer_index,
            parent_root: hash(&self.parent_root)?,
            state_root: hash(&self.state_root)?,
            body_root: hash(&self.body_root)?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JsonValidator {
    attestation_public_key: String,
    proposal_public_key: String,
    index: u64,
}

impl JsonValidator {
    pub fn to_validator(&self) -> Result<Validator, String> {
        let public_key = |value: &str| {
            PublicKeyBytes::new(decode_hex(value)?).map_err(|e| format!("{value}: {e:?}"))
        };
        Ok(Validator {
            attestation_public_key: public_key(&self.attestation_public_key)?,
            proposal_public_key: public_key(&self.proposal_public_key)?,
            index: self.index,
        })
    }
}

pub fn validators(values: &[JsonValidator]) -> Result<Validators, String> {
    let validators = values
        .iter()
        .map(JsonValidator::to_validator)
        .collect::<Result<Vec<_>, _>>()?;
    Validators::new(validators).map_err(|e| format!("{e:?}"))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JsonGenesisConfig {
    genesis_time: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JsonState {
    config: JsonGenesisConfig,
    slot: u64,
    latest_block_header: JsonBlockHeader,
    latest_justified: JsonCheckpoint,
    latest_finalized: JsonCheckpoint,
    historical_block_hashes: JsonList<String>,
    justified_slots: JsonList<bool>,
    validators: JsonList<JsonValidator>,
    justifications_roots: JsonList<String>,
    justifications_validators: JsonList<bool>,
}

impl JsonState {
    pub fn to_state(&self) -> Result<State, String> {
        Ok(State {
            config: GenesisConfig {
                genesis_time: self.config.genesis_time,
            },
            slot: Slot::new(self.slot),
            latest_block_header: self.latest_block_header.to_block_header()?,
            latest_justified: self.latest_justified.to_checkpoint()?,
            latest_finalized: self.latest_finalized.to_checkpoint()?,
            historical_block_hashes: hashes(&self.historical_block_hashes.data)?,
            justified_slots: bitlist(&self.justified_slots.data)?,
            validators: validators(&self.validators.data)?,
            justifications_roots: hashes(&self.justifications_roots.data)?,
            justifications_validators: bitlist(&self.justifications_validators.data)?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonBytes {
    data: String,
}

impl JsonBytes {
    pub fn to_proof_bytes(&self) -> Result<ProofBytes, String> {
        ProofBytes::new(decode_hex(&self.data)?).map_err(|e| format!("{e:?}"))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonMultiMessageAggregate {
    proof: JsonBytes,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JsonSignedBlock {
    block: JsonBlock,
    proof: JsonMultiMessageAggregate,
}

impl JsonSignedBlock {
    pub fn to_signed_block(&self) -> Result<SignedBlock, String> {
        Ok(SignedBlock {
            block: self.block.to_block()?,
            proof: MultiMessageAggregate {
                proof: self.proof.proof.to_proof_bytes()?,
            },
        })
    }
}
