//! Deliberately naive transcription of the consensus-specs fork choice head selection, for
//! differential testing against `proto_array`. Not optimized, not for production use.
//!
//! Transcribed from consensus-specs `v1.7.0-alpha.11`:
//! - `specs/gloas/fork-choice.md` (head selection, payload status, PTC helpers)
//! - `specs/phase0/fork-choice.md` (`get_attestation_score`, proposer score, `is_head_weak`)
//!
//! Functions mirror the spec's names and structure line-for-line where possible. The spec's
//! `get_filtered_block_tree` is NOT transcribed: scenarios provide the block tree directly and
//! every block is assumed viable (checkpoint filtering is out of scope).
//!
//! Panics on malformed scenarios (missing parents, dangling roots), mirroring the spec's dict
//! access and assert semantics. This crate must only ever be a dev-dependency.
//!
//! The transcription itself is certified against the executable pyspec by
//! `make certify-fork-choice` (see `certify.py` in this crate's directory) — run it whenever the
//! pinned spec version changes.

mod get_head;
mod store;

pub use get_head::{get_head, get_proposer_head};
pub use store::{Block, ForkChoiceNode, LatestMessage, PayloadStatus, Store};
