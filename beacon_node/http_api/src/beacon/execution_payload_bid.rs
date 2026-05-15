use crate::task_spawner::{Priority, TaskSpawner};
use crate::utils::{ChainFilter, EthV1Filter, NetworkTxFilter, ResponseFilter, TaskSpawnerFilter};
use beacon_chain::{BeaconChain, BeaconChainTypes};
use bytes::Bytes;
use lighthouse_network::PubsubMessage;
use network::NetworkMessage;
use ssz::Decode;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info, warn};
use types::SignedExecutionPayloadBid;
use warp::Filter;

/// POST beacon/execution_payload_bid
pub(crate) fn post_beacon_execution_payload_bid<T: BeaconChainTypes>(
    eth_v1: EthV1Filter,
    task_spawner_filter: TaskSpawnerFilter<T>,
    chain_filter: ChainFilter<T>,
    network_tx_filter: NetworkTxFilter<T>,
) -> ResponseFilter {
    eth_v1
        .and(warp::path("beacon"))
        .and(warp::path("execution_payload_bid"))
        .and(warp::path::end())
        .and(warp::body::json())
        .and(task_spawner_filter)
        .and(chain_filter)
        .and(network_tx_filter)
        .then(
            |bid: SignedExecutionPayloadBid<T::EthSpec>,
             task_spawner: TaskSpawner<T::EthSpec>,
             chain: Arc<BeaconChain<T>>,
             network_tx: UnboundedSender<NetworkMessage<T::EthSpec>>| {
                task_spawner.blocking_json_task(Priority::P0, move || {
                    publish_execution_payload_bid(bid, &chain, &network_tx)
                })
            },
        )
        .boxed()
}

/// POST beacon/execution_payload_bid (SSZ)
pub(crate) fn post_beacon_execution_payload_bid_ssz<T: BeaconChainTypes>(
    eth_v1: EthV1Filter,
    task_spawner_filter: TaskSpawnerFilter<T>,
    chain_filter: ChainFilter<T>,
    network_tx_filter: NetworkTxFilter<T>,
) -> ResponseFilter {
    eth_v1
        .and(warp::path("beacon"))
        .and(warp::path("execution_payload_bid"))
        .and(warp::path::end())
        .and(warp::header::exact(
            eth2::CONTENT_TYPE_HEADER,
            eth2::SSZ_CONTENT_TYPE_HEADER,
        ))
        .and(warp::body::bytes())
        .and(task_spawner_filter)
        .and(chain_filter)
        .and(network_tx_filter)
        .then(
            |body_bytes: Bytes,
             task_spawner: TaskSpawner<T::EthSpec>,
             chain: Arc<BeaconChain<T>>,
             network_tx: UnboundedSender<NetworkMessage<T::EthSpec>>| {
                task_spawner.blocking_json_task(Priority::P0, move || {
                    let bid =
                        SignedExecutionPayloadBid::<T::EthSpec>::from_ssz_bytes(&body_bytes)
                            .map_err(|e| {
                                warp_utils::reject::custom_bad_request(format!(
                                    "invalid SSZ: {e:?}"
                                ))
                            })?;
                    publish_execution_payload_bid(bid, &chain, &network_tx)
                })
            },
        )
        .boxed()
}

/// Verify and publish a signed execution payload bid.
fn publish_execution_payload_bid<T: BeaconChainTypes>(
    bid: SignedExecutionPayloadBid<T::EthSpec>,
    chain: &BeaconChain<T>,
    network_tx: &UnboundedSender<NetworkMessage<T::EthSpec>>,
) -> Result<(), warp::Rejection> {
    let slot = bid.message.slot;
    let builder_index = bid.message.builder_index;

    info!(
        %slot,
        %builder_index,
        value = bid.message.value,
        "Received execution payload bid via HTTP API"
    );

    let bid = Arc::new(bid);

    // Verify the bid for gossip (this also inserts into the bid cache and emits SSE event)
    let _verified = chain
        .verify_payload_bid_for_gossip(bid.clone())
        .map_err(|e| {
            warn!(
                %slot,
                %builder_index,
                error = ?e,
                "Execution payload bid failed gossip verification"
            );
            warp_utils::reject::custom_bad_request(format!(
                "bid failed gossip verification: {e}"
            ))
        })?;

    // Broadcast to P2P network
    crate::utils::publish_pubsub_message(
        network_tx,
        PubsubMessage::ExecutionPayloadBid(Box::new((*bid).clone())),
    )?;

    debug!(
        %slot,
        %builder_index,
        "Successfully published execution payload bid to network"
    );

    Ok(())
}
