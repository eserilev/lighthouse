use crate::local_network::{EXECUTION_PORT, TERMINAL_BLOCK, TERMINAL_DIFFICULTY};
use crate::{checks, LocalNetwork};
use clap::ArgMatches;
use eth1::{Eth1Endpoint, DEFAULT_CHAIN_ID};
use eth1_test_rig::AnvilEth1Instance;

use crate::retry::with_retry;
use execution_layer::http::deposit_methods::Eth1Id;
use futures::prelude::*;
use node_test_rig::environment::RuntimeContext;
use node_test_rig::{
    environment::{EnvironmentBuilder, LoggerConfig},
    testing_client_config, testing_validator_config, ApiTopic, ClientConfig, ClientGenesis,
    ValidatorFiles,
};
use rayon::prelude::*;
use sensitive_url::SensitiveUrl;
use std::cmp::max;
use std::net::Ipv4Addr;
use std::time::Duration;
use tokio::time::sleep;
use types::{Epoch, EthSpec, MinimalEthSpec};

const END_EPOCH: u64 = 16;
const ALTAIR_FORK_EPOCH: u64 = 1;
const BELLATRIX_FORK_EPOCH: u64 = 2;

const SUGGESTED_FEE_RECIPIENT: [u8; 20] =
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

pub fn run_eth1_sim(matches: &ArgMatches) -> Result<(), String> {
    let node_count = matches
        .get_one::<String>("nodes")
        .expect("missing nodes default")
        .parse::<usize>()
        .expect("missing nodes default");
    let proposer_nodes = *matches.get_one::<usize>("proposer-nodes").unwrap_or(&0);
    println!("PROPOSER-NODES: {}", proposer_nodes);
    let validators_per_node = matches
        .get_one::<String>("validators_per_node")
        .expect("missing validators_per_node default")
        .parse::<usize>()
        .expect("missing validators_per_node default");
    let speed_up_factor = matches
        .get_one::<String>("speed_up_factor")
        .expect("missing speed_up_factor default")
        .parse::<u64>()
        .expect("missing speed_up_factor default");
    let continue_after_checks = matches.get_flag("continue_after_checks");
    let post_merge_sim = matches.get_flag("post-merge");

    println!("Beacon Chain Simulator:");
    println!(" nodes:{}, proposer_nodes: {}", node_count, proposer_nodes);

    println!(" validators_per_node:{}", validators_per_node);
    println!(" post merge simulation:{}", post_merge_sim);
    println!(" continue_after_checks:{}", continue_after_checks);

    // Generate the directories and keystores required for the validator clients.
    let validator_files = (0..node_count)
        .into_par_iter()
        .map(|i| {
            println!(
                "Generating keystores for validator {} of {}",
                i + 1,
                node_count
            );

            let indices =
                (i * validators_per_node..(i + 1) * validators_per_node).collect::<Vec<_>>();
            ValidatorFiles::with_keystores(&indices).unwrap()
        })
        .collect::<Vec<_>>();

    let mut env = EnvironmentBuilder::minimal()
        .initialize_logger(LoggerConfig {
            path: None,
            debug_level: String::from("debug"),
            logfile_debug_level: String::from("debug"),
            log_format: None,
            logfile_format: None,
            log_color: false,
            disable_log_timestamp: false,
            max_log_size: 0,
            max_log_number: 0,
            compression: false,
            is_restricted: true,
            sse_logging: false,
        })?
        .multi_threaded_tokio_runtime()?
        .build()?;

    let eth1_block_time = Duration::from_millis(15_000 / speed_up_factor);

    let spec = &mut env.eth2_config.spec;

    let total_validator_count = validators_per_node * node_count;
    let altair_fork_version = spec.altair_fork_version;
    let bellatrix_fork_version = spec.bellatrix_fork_version;

    spec.seconds_per_slot /= speed_up_factor;
    spec.seconds_per_slot = max(1, spec.seconds_per_slot);
    spec.eth1_follow_distance = 16;
    spec.genesis_delay = eth1_block_time.as_secs() * spec.eth1_follow_distance * 2;
    spec.min_genesis_time = 0;
    spec.min_genesis_active_validator_count = total_validator_count as u64;
    spec.seconds_per_eth1_block = eth1_block_time.as_secs();
    spec.altair_fork_epoch = Some(Epoch::new(ALTAIR_FORK_EPOCH));
    // Set these parameters only if we are doing a merge simulation
    if post_merge_sim {
        spec.terminal_total_difficulty = TERMINAL_DIFFICULTY.into();
        spec.bellatrix_fork_epoch = Some(Epoch::new(BELLATRIX_FORK_EPOCH));
    }

    let seconds_per_slot = spec.seconds_per_slot;
    let slot_duration = Duration::from_secs(spec.seconds_per_slot);
    let initial_validator_count = spec.min_genesis_active_validator_count as usize;
    let deposit_amount = env.eth2_config.spec.max_effective_balance;

    let context = env.core_context();

    let main_future = async {
        /*
         * Create a new `LocalNetwork` with one beacon node.
         */
        let max_retries = 3;
        let (network, beacon_config) = with_retry(max_retries, || {
            Box::pin(create_local_network(
                LocalNetworkParams {
                    eth1_block_time,
                    total_validator_count,
                    deposit_amount,
                    node_count,
                    proposer_nodes,
                    post_merge_sim,
                },
                context.clone(),
            ))
        })
        .await?;

        /*
         * One by one, add beacon nodes to the network.
         */
        for _ in 0..node_count - 1 {
            network
                .add_beacon_node(beacon_config.clone(), false)
                .await?;
        }

        /*
         * One by one, add proposer nodes to the network.
         */
        for _ in 0..proposer_nodes - 1 {
            println!("Adding a proposer node");
            network.add_beacon_node(beacon_config.clone(), true).await?;
        }

        /*
         * One by one, add validators to the network.
         */

        let executor = context.executor.clone();
        for (i, files) in validator_files.into_iter().enumerate() {
            let network_1 = network.clone();
            executor.spawn(
                async move {
                    let mut validator_config = testing_validator_config();
                    if post_merge_sim {
                        validator_config.fee_recipient = Some(SUGGESTED_FEE_RECIPIENT.into());
                    }
                    println!("Adding validator client {}", i);

                    // Enable broadcast on every 4th node.
                    if i % 4 == 0 {
                        validator_config.broadcast_topics = ApiTopic::all();
                        let beacon_nodes = vec![i, (i + 1) % node_count];
                        network_1
                            .add_validator_client_with_fallbacks(
                                validator_config,
                                i,
                                beacon_nodes,
                                files,
                            )
                            .await
                    } else {
                        network_1
                            .add_validator_client(validator_config, i, files, i % 2 == 0)
                            .await
                    }
                    .expect("should add validator");
                },
                "vc",
            );
        }

        let duration_to_genesis = network.duration_to_genesis().await;
        println!("Duration to genesis: {}", duration_to_genesis.as_secs());
        sleep(duration_to_genesis).await;

        if post_merge_sim {
            let executor = executor.clone();
            let network_2 = network.clone();
            executor.spawn(
                async move {
                    println!("Mining pow blocks");
                    let mut interval = tokio::time::interval(Duration::from_secs(seconds_per_slot));
                    for i in 1..=TERMINAL_BLOCK + 1 {
                        interval.tick().await;
                        let _ = network_2.mine_pow_blocks(i);
                    }
                },
                "pow_mining",
            );
        }
        /*
         * Start the checks that ensure the network performs as expected.
         *
         * We start these checks immediately after the validators have started. This means we're
         * relying on the validator futures to all return immediately after genesis so that these
         * tests start at the right time. Whilst this is works well for now, it's subject to
         * breakage by changes to the VC.
         */

        let (
            finalization,
            block_prod,
            validator_count,
            onboarding,
            fork,
            sync_aggregate,
            transition,
            light_client_update,
        ) = futures::join!(
            // Check that the chain finalizes at the first given opportunity.
            checks::verify_first_finalization(network.clone(), slot_duration),
            // Check that a block is produced at every slot.
            checks::verify_full_block_production_up_to(
                network.clone(),
                Epoch::new(END_EPOCH).start_slot(MinimalEthSpec::slots_per_epoch()),
                slot_duration,
            ),
            // Check that the chain starts with the expected validator count.
            checks::verify_initial_validator_count(
                network.clone(),
                slot_duration,
                initial_validator_count,
            ),
            // Check that validators greater than `spec.min_genesis_active_validator_count` are
            // onboarded at the first possible opportunity.
            checks::verify_validator_onboarding(
                network.clone(),
                slot_duration,
                total_validator_count,
            ),
            // Check that all nodes have transitioned to the required fork.
            checks::verify_fork_version(
                network.clone(),
                if post_merge_sim {
                    Epoch::new(BELLATRIX_FORK_EPOCH)
                } else {
                    Epoch::new(ALTAIR_FORK_EPOCH)
                },
                slot_duration,
                if post_merge_sim {
                    bellatrix_fork_version
                } else {
                    altair_fork_version
                }
            ),
            // Check that all sync aggregates are full.
            checks::verify_full_sync_aggregates_up_to(
                network.clone(),
                // Start checking for sync_aggregates at `FORK_EPOCH + 1` to account for
                // inefficiencies in finding subnet peers at the `fork_slot`.
                Epoch::new(ALTAIR_FORK_EPOCH + 1).start_slot(MinimalEthSpec::slots_per_epoch()),
                Epoch::new(END_EPOCH).start_slot(MinimalEthSpec::slots_per_epoch()),
                slot_duration,
            ),
            // Check that the transition block is finalized.
            checks::verify_transition_block_finalized(
                network.clone(),
                Epoch::new(TERMINAL_BLOCK / MinimalEthSpec::slots_per_epoch()),
                slot_duration,
                post_merge_sim
            ),
            checks::verify_light_client_updates(
                network.clone(),
                // Sync aggregate available from slot 1 after Altair fork transition.
                Epoch::new(ALTAIR_FORK_EPOCH).start_slot(MinimalEthSpec::slots_per_epoch()) + 1,
                Epoch::new(END_EPOCH).start_slot(MinimalEthSpec::slots_per_epoch()),
                slot_duration
            )
        );

        block_prod?;
        finalization?;
        validator_count?;
        onboarding?;
        fork?;
        sync_aggregate?;
        transition?;
        light_client_update?;

        // The `final_future` either completes immediately or never completes, depending on the value
        // of `continue_after_checks`.

        if continue_after_checks {
            future::pending::<()>().await;
        }
        /*
         * End the simulation by dropping the network. This will kill all running beacon nodes and
         * validator clients.
         */
        println!(
            "Simulation complete. Finished with {} beacon nodes and {} validator clients",
            network.beacon_node_count() + network.proposer_node_count(),
            network.validator_client_count()
        );

        // Be explicit about dropping the network, as this kills all the nodes. This ensures
        // all the checks have adequate time to pass.
        drop(network);
        Ok::<(), String>(())
    };

    env.runtime().block_on(main_future).unwrap();

    env.fire_signal();
    env.shutdown_on_idle();

    Ok(())
}

struct LocalNetworkParams {
    eth1_block_time: Duration,
    total_validator_count: usize,
    deposit_amount: u64,
    node_count: usize,
    proposer_nodes: usize,
    post_merge_sim: bool,
}

async fn create_local_network<E: EthSpec>(
    LocalNetworkParams {
        eth1_block_time,
        total_validator_count,
        deposit_amount,
        node_count,
        proposer_nodes,
        post_merge_sim,
    }: LocalNetworkParams,
    context: RuntimeContext<E>,
) -> Result<(LocalNetwork<E>, ClientConfig), String> {
    /*
     * Deploy the deposit contract, spawn tasks to keep creating new blocks and deposit
     * validators.
     */
    let anvil_eth1_instance = AnvilEth1Instance::new(DEFAULT_CHAIN_ID.into()).await?;
    let deposit_contract = anvil_eth1_instance.deposit_contract;
    let chain_id = anvil_eth1_instance.anvil.chain_id();
    let anvil = anvil_eth1_instance.anvil;
    let eth1_endpoint =
        SensitiveUrl::parse(anvil.endpoint().as_str()).expect("Unable to parse anvil endpoint.");
    let deposit_contract_address = deposit_contract.address();

    // Start a timer that produces eth1 blocks on an interval.
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(eth1_block_time);
        loop {
            interval.tick().await;
            let _ = anvil.evm_mine().await;
        }
    });

    // Submit deposits to the deposit contract.
    tokio::spawn(async move {
        for i in 0..total_validator_count {
            println!("Submitting deposit for validator {}...", i);
            let _ = deposit_contract
                .deposit_deterministic_async::<E>(i, deposit_amount)
                .await;
        }
    });

    let mut beacon_config = testing_client_config();

    beacon_config.genesis = ClientGenesis::DepositContract;
    beacon_config.eth1.endpoint = Eth1Endpoint::NoAuth(eth1_endpoint);
    beacon_config.eth1.deposit_contract_address = deposit_contract_address;
    beacon_config.eth1.deposit_contract_deploy_block = 0;
    beacon_config.eth1.lowest_cached_block_number = 0;
    beacon_config.eth1.follow_distance = 1;
    beacon_config.eth1.node_far_behind_seconds = 20;
    beacon_config.dummy_eth1_backend = false;
    beacon_config.sync_eth1_chain = true;
    beacon_config.eth1.auto_update_interval_millis = eth1_block_time.as_millis() as u64;
    beacon_config.eth1.chain_id = Eth1Id::from(chain_id);
    beacon_config.network.target_peers = node_count + proposer_nodes - 1;

    beacon_config.network.enr_address = (Some(Ipv4Addr::LOCALHOST), None);
    beacon_config.network.enable_light_client_server = true;
    beacon_config.chain.enable_light_client_server = true;
    beacon_config.http_api.enable_light_client_server = true;

    if post_merge_sim {
        let el_config = execution_layer::Config {
            execution_endpoints: vec![SensitiveUrl::parse(&format!(
                "http://localhost:{}",
                EXECUTION_PORT
            ))
            .unwrap()],
            ..Default::default()
        };

        beacon_config.execution_layer = Some(el_config);
    }

    let network = LocalNetwork::new(context, beacon_config.clone()).await?;
    Ok((network, beacon_config))
}
