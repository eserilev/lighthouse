#!/usr/bin/env python3
"""Certify `fork_choice_reference` against the executable consensus-specs pyspec.

Consumes the versioned JSONL produced by the `export_certification_scenarios` test in
`proto_array` (see `make certify-fork-choice`) and replays each scenario through the pyspec's
`get_head`, comparing against the Rust reference's answer.

Requires the pinned pyspec package:

    pip install eth-consensus-specs==1.7.0a11
"""

import json
import sys

FORMAT_VERSION = 2
SPEC_VERSION = "v1.7.0-alpha.11"
PIP_VERSION = "1.7.0a11"


def load_spec():
    from importlib.metadata import PackageNotFoundError, version

    try:
        installed = version("eth-consensus-specs")
    except PackageNotFoundError:
        sys.exit(f"eth-consensus-specs not installed: pip install eth-consensus-specs=={PIP_VERSION}")
    if installed != PIP_VERSION:
        sys.exit(
            f"eth-consensus-specs {installed} installed but {PIP_VERSION} required "
            f"(the Rust reference is transcribed from {SPEC_VERSION})"
        )
    from eth_consensus_specs.gloas import minimal as spec

    return spec


def root(spec, hex_str):
    return spec.Root(bytes.fromhex(hex_str[2:]))


def block_hash(spec, hex_str):
    return spec.Hash32(bytes.fromhex(hex_str[2:]))


def checkpoint(spec, cp):
    return spec.Checkpoint(epoch=spec.Epoch(cp["epoch"]), root=root(spec, cp["root"]))


def build_state(spec, balances):
    state = spec.BeaconState()
    for balance in balances:
        state.validators.append(
            spec.Validator(
                effective_balance=spec.Gwei(balance),
                activation_eligibility_epoch=spec.Epoch(0),
                activation_epoch=spec.Epoch(0),
                exit_epoch=spec.FAR_FUTURE_EPOCH,
                withdrawable_epoch=spec.FAR_FUTURE_EPOCH,
            )
        )
        state.balances.append(spec.Gwei(balance))
    return state


def build_block(spec, block):
    beacon_block = spec.BeaconBlock(
        slot=spec.Slot(block["slot"]),
        proposer_index=spec.ValidatorIndex(block["proposer_index"]),
        parent_root=root(spec, block["parent_root"]),
    )
    bid = beacon_block.body.signed_execution_payload_bid.message
    bid.block_hash = block_hash(spec, block["bid_block_hash"])
    bid.parent_block_hash = block_hash(spec, block["bid_parent_block_hash"])
    return beacon_block


def build_store(spec, header, scenario):
    state = build_state(spec, scenario["balances"])
    store_justified = checkpoint(spec, scenario["store_justified_checkpoint"])
    store_finalized = checkpoint(spec, scenario["store_finalized_checkpoint"])

    # One state per distinct per-block justified checkpoint, so `get_voting_source` reads the
    # block's own justified checkpoint.
    justified_states = {}

    def state_with_justified(cp):
        key = (int(cp.epoch), bytes(cp.root))
        if key not in justified_states:
            variant = state.copy()
            variant.current_justified_checkpoint = cp
            justified_states[key] = variant
        return justified_states[key]

    blocks = {}
    block_states = {}
    block_timeliness = {}
    unrealized_justifications = {}
    payload_timeliness_vote = {}
    payload_data_availability_vote = {}
    for block in scenario["blocks"]:
        r = root(spec, block["root"])
        blocks[r] = build_block(spec, block)
        block_states[r] = state_with_justified(checkpoint(spec, block["justified_checkpoint"]))
        block_timeliness[r] = [True, True]
        unrealized_justifications[r] = checkpoint(
            spec, block["unrealized_justified_checkpoint"]
        )
        payload_timeliness_vote[r] = [None] * spec.PTC_SIZE
        payload_data_availability_vote[r] = [None] * spec.PTC_SIZE

    payloads = {}
    for revealed in scenario["revealed"]:
        payloads[root(spec, revealed)] = spec.ExecutionPayloadEnvelope()

    for block_root_hex, votes in scenario["ptc_votes"].items():
        r = root(spec, block_root_hex)
        for i, (timely, available) in enumerate(votes):
            payload_timeliness_vote[r][i] = timely
            payload_data_availability_vote[r][i] = available

    latest_messages = {}
    for validator, message in scenario["latest_messages"].items():
        latest_messages[spec.ValidatorIndex(int(validator))] = spec.LatestMessage(
            slot=spec.Slot(message["slot"]),
            root=root(spec, message["root"]),
            payload_present=message["payload_present"],
        )

    seconds_per_slot = spec.config.SLOT_DURATION_MS // 1000
    return spec.Store(
        time=spec.uint64(scenario["current_slot"] * seconds_per_slot),
        genesis_time=spec.uint64(0),
        justified_checkpoint=store_justified,
        finalized_checkpoint=store_finalized,
        unrealized_justified_checkpoint=store_justified,
        unrealized_finalized_checkpoint=store_finalized,
        proposer_boost_root=root(spec, scenario["proposer_boost_root"]),
        equivocating_indices={
            spec.ValidatorIndex(i) for i in scenario["equivocating_indices"]
        },
        blocks=blocks,
        block_states=block_states,
        block_timeliness=block_timeliness,
        checkpoint_states={store_justified: state},
        latest_messages=latest_messages,
        unrealized_justifications=unrealized_justifications,
        payloads=payloads,
        payload_timeliness_vote=payload_timeliness_vote,
        payload_data_availability_vote=payload_data_availability_vote,
    )


def main():
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <scenarios.jsonl>")

    spec = load_spec()
    status_names = {
        spec.PAYLOAD_STATUS_EMPTY: "empty",
        spec.PAYLOAD_STATUS_FULL: "full",
        spec.PAYLOAD_STATUS_PENDING: "pending",
    }

    with open(sys.argv[1]) as f:
        lines = [json.loads(line) for line in f if line.strip()]

    header, scenarios = lines[0], lines[1:]
    if header["format_version"] != FORMAT_VERSION:
        sys.exit(f"unsupported format version {header['format_version']}")
    if header["spec_version"] != SPEC_VERSION:
        sys.exit(f"scenarios exported for {header['spec_version']}, script pins {SPEC_VERSION}")
    assert header["ptc_size"] == spec.PTC_SIZE
    assert header["slots_per_epoch"] == spec.SLOTS_PER_EPOCH
    assert header["proposer_score_boost"] == spec.config.PROPOSER_SCORE_BOOST
    assert header["reorg_head_weight_threshold"] == spec.config.REORG_HEAD_WEIGHT_THRESHOLD

    mismatches = 0
    for index, scenario in enumerate(scenarios):
        store = build_store(spec, header, scenario)
        head = spec.get_head(store)
        got = (f"0x{bytes(head.root).hex()}", status_names[head.payload_status])
        expected = (scenario["expected_head_root"], scenario["expected_payload_status"])
        if got != expected:
            mismatches += 1
            print(f"MISMATCH at scenario {index}: pyspec {got} != reference {expected}")
            print(f"  {json.dumps(scenario)}")

    print(f"certified {len(scenarios)} scenarios against pyspec {SPEC_VERSION}: "
          f"{mismatches} mismatches")
    sys.exit(1 if mismatches else 0)


if __name__ == "__main__":
    main()


