//! # Skip-Slots
//!
//! Use this tool to process a `BeaconState` through empty slots. Useful for benchmarking or
//! troubleshooting consensus failures.
//!
//! It can load states from file or pull them from a beaconAPI. States pulled from a beaconAPI can
//! be saved to disk to reduce future calls to that server.
//!
//! ## Examples
//!
//! ### Example 1.
//!
//! Download a state from a HTTP endpoint and skip forward an epoch, twice (the initial state is
//! advanced 32 slots twice, rather than it being advanced 64 slots):
//!
//! ```ignore
//! lcli skip-slots \
//!     --beacon-url http://localhost:5052 \
//!     --state-id 0x3cdc33cd02713d8d6cc33a6dbe2d3a5bf9af1d357de0d175a403496486ff845e \\
//!     --slots 32 \
//!     --runs 2
//! ```
//!
//! ### Example 2.
//!
//! Download a state to a SSZ file (without modifying it):
//!
//! ```ignore
//! lcli skip-slots \
//!     --beacon-url http://localhost:5052 \
//!     --state-id 0x3cdc33cd02713d8d6cc33a6dbe2d3a5bf9af1d357de0d175a403496486ff845e \
//!     --slots 0 \
//!     --runs 0 \
//!     --output-path /tmp/state-0x3cdc.ssz
//! ```
//!
//! ### Example 3.
//!
//! Do two runs over the state that was downloaded in the previous example:
//!
//! ```ignore
//! lcli skip-slots \
//!     --pre-state-path /tmp/state-0x3cdc.ssz \
//!     --slots 32 \
//!     --runs 2
//! ```
use crate::transition_blocks::load_from_ssz_with;
use clap::ArgMatches;
use clap_utils::{parse_optional, parse_required};
use environment::Environment;
use eth2::{types::StateId, BeaconNodeHttpClient, SensitiveUrl, Timeouts};
use eth2_network_config::Eth2NetworkConfig;
use log::info;
use ssz::Encode;
use state_processing::state_advance::{complete_state_advance, partial_state_advance};
use state_processing::AllCaches;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use types::{BeaconState, EthSpec, Hash256, Slot};
use types::milhouse::mem::MemoryTracker;

const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

pub fn run<E: EthSpec>(
    env: Environment<E>,
    network_config: Eth2NetworkConfig,
    matches: &ArgMatches,
) -> Result<(), String> {
    let spec = &network_config.chain_spec::<E>()?;

    // the current slot to check
    let current_slot: u64 = parse_required(matches, "current-slot")?;
    let beacon_url: SensitiveUrl = parse_required(matches, "beacon-url")?;
    // slots to subtract from the `check_slot`
    let slots: u64 = parse_required(matches, "slots")?;
    // amount of beacon states we are fetching
    let runs: usize = parse_required(matches, "runs")?;
    let client = BeaconNodeHttpClient::new(beacon_url, Timeouts::set_all(HTTP_TIMEOUT));

    info!("Using {} spec", E::spec_name());
    info!("Advancing {} slots", slots);
    let mut mem_tracker = MemoryTracker::default();
    let mut states = vec![];
    let mut total_usage = 0;
    let mut check_slot = current_slot;
    for _ in 0..runs {
        println!("check slot {}", check_slot);
        let state_id = StateId::Slot(Slot::new(check_slot));
        let state = get_state(&client, &env, state_id)?;
        println!("state slot {}", state.slot());
        states.push(state);
        check_slot -= slots;
    }

    for state in states {
        let stats = mem_tracker.track_item(&state);
        total_usage += stats.differential_size;
        println!("diff size: {}", stats.differential_size);
        println!("total usage: {}", total_usage);
    }

    Ok(())
}


fn get_state<E: EthSpec>(client: &BeaconNodeHttpClient, env: &Environment<E>, state_id: StateId) -> Result<BeaconState<E>, String> {
    let executor = env.core_context().executor;
    let state = executor
        .handle()
        .ok_or("shutdown in progress")?
        .block_on(async move {
            client
                .get_debug_beacon_states::<E>(state_id)
                .await
                .map_err(|e| format!("Failed to download state: {:?}", e))
        })
        .map_err(|e| format!("Failed to complete task: {:?}", e))?
        .ok_or_else(|| format!("Unable to locate state at {:?}", state_id))?
        .data;
    println!("{:?}", state_id);
    let state_root = match state_id {
        StateId::Root(root) => Some(root),
        _ => None,
    };
    Ok(state)
}
