#!/usr/bin/env python3
"""Certify the ForkChoice wrapper harness against the pyspec's fork choice handlers.

Consumes the versioned JSONL produced by `export_handler_sequences` in the `fork_choice`
crate's wrapper differential (see `make certify-fork-choice-handlers`) and replays each
recorded handler-call sequence through the actual pyspec, comparing the head and the store's
justified checkpoint at every query Lighthouse answered.

Real pyspec code is used for everything fork choice observes:

- `on_tick` drives time (proposer boost clearing, epoch-boundary checkpoint pull-ups).
- Block imports replay `on_block`'s store mutations verbatim (`record_block_timeliness`,
  `update_proposer_boost_root`, `update_checkpoints`, `compute_pulled_up_tip`) minus
  `state_transition`: post-states are synthesized with `process_slots` from the parent state,
  since the harness's blocks are skeletal. Crafted previous-epoch participation is mirrored so
  `compute_pulled_up_tip` derives the same unrealized justification.
- Attestations run `validate_on_attestation` + `store_target_checkpoint_state` +
  `update_latest_messages`. Committee expansion (`get_indexed_attestation`) is bypassed: the
  harness feeds Lighthouse pre-indexed attestations, so the attesting index is taken from the
  export. An attestation the spec rejects as same-slot is redelivered after the next tick,
  which is the queueing behaviour Lighthouse implements internally.
- Attester slashings poke `store.equivocating_indices` directly (the entire store effect of
  `on_attester_slashing`; the intersection logic is covered by the Rust differential).
- Payload reveals insert into `store.payloads` (the store effect of a verified envelope).

Requires the pinned pyspec package:

    pip install eth-consensus-specs==1.7.0a11
"""

import json
import sys

FORMAT_VERSION = 1
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
            f"(the wrapper harness pins {SPEC_VERSION})"
        )
    from eth_consensus_specs.gloas import minimal as spec

    return spec


def root(spec, hex_str):
    return spec.Root(bytes.fromhex(hex_str[2:]))


def block_hash(spec, hex_str):
    return spec.Hash32(bytes.fromhex(hex_str[2:]))


def build_anchor_state(spec, balances):
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
        state.previous_epoch_participation.append(spec.ParticipationFlags(0))
        state.current_epoch_participation.append(spec.ParticipationFlags(0))
        state.inactivity_scores.append(spec.uint64(0))
    return state


class Replay:
    def __init__(self, spec, scenario):
        self.spec = spec
        self.mismatches = []
        self.pending = []
        anchor_root = root(spec, scenario["anchor_root"])
        anchor_cp = spec.Checkpoint(epoch=spec.Epoch(0), root=anchor_root)
        anchor_state = build_anchor_state(spec, scenario["balances"])
        anchor_block = spec.BeaconBlock()
        # Pristine post-states by root, shared across store copies (handlers copy on read).
        self.states = {anchor_root: anchor_state}
        self.block_slots = {anchor_root: 0}
        self.store = spec.Store(
            time=spec.uint64(0),
            genesis_time=spec.uint64(0),
            justified_checkpoint=anchor_cp,
            finalized_checkpoint=anchor_cp,
            unrealized_justified_checkpoint=anchor_cp,
            unrealized_finalized_checkpoint=anchor_cp,
            proposer_boost_root=spec.Root(),
            equivocating_indices=set(),
            blocks={anchor_root: anchor_block},
            block_states={anchor_root: anchor_state},
            block_timeliness={anchor_root: [True, True]},
            checkpoint_states={anchor_cp: anchor_state},
            latest_messages={},
            unrealized_justifications={anchor_root: anchor_cp},
            payloads={},
            payload_timeliness_vote={anchor_root: [None] * spec.PTC_SIZE},
            payload_data_availability_vote={anchor_root: [None] * spec.PTC_SIZE},
        )

    def seconds_at(self, slot, late):
        seconds_per_slot = self.spec.config.SLOT_DURATION_MS // 1000
        return slot * seconds_per_slot + (4 if late else 0)

    def tick_to(self, seconds):
        # Never move time backwards (a query at a slot a late block was imported into).
        if seconds > self.store.time:
            self.spec.on_tick(self.store, self.spec.uint64(seconds))
        self.flush_pending()

    def flush_pending(self):
        still_pending = []
        for attestation, validator in sorted(
            self.pending, key=lambda entry: int(entry[0].data.slot)
        ):
            if self.spec.get_current_slot(self.store) >= attestation.data.slot + 1:
                self.apply_attestation(attestation, validator)
            else:
                still_pending.append((attestation, validator))
        self.pending = still_pending

    def apply_attestation(self, attestation, validator):
        spec = self.spec
        spec.validate_on_attestation(self.store, attestation, is_from_block=False)
        spec.store_target_checkpoint_state(self.store, attestation.data.target)
        spec.update_latest_messages(
            self.store, [spec.ValidatorIndex(validator)], attestation
        )

    def replay_block(self, event):
        spec = self.spec
        r = root(spec, event["root"])
        parent_root = root(spec, event["parent_root"])
        slot = event["slot"]
        self.tick_to(self.seconds_at(event["import_slot"], event["late"]))

        parent_state = self.states[parent_root]
        state = parent_state.copy()
        if state.slot < slot:
            spec.process_slots(state, spec.Slot(slot))
            # The harness's block roots are Lighthouse's, not hashes of these synthesized
            # states; patch the advanced range so `get_block_root` agrees with the export.
            for s in range(self.block_slots[parent_root], slot):
                state.block_roots[s % spec.SLOTS_PER_HISTORICAL_ROOT] = parent_root
        state.latest_block_header = spec.BeaconBlockHeader(
            slot=spec.Slot(slot),
            proposer_index=spec.ValidatorIndex(event["proposer"]),
            parent_root=parent_root,
        )
        if event["justify_previous_epoch"]:
            flags = spec.ParticipationFlags(2**spec.TIMELY_TARGET_FLAG_INDEX)
            for i in range(len(state.previous_epoch_participation)):
                state.previous_epoch_participation[i] = flags
        self.states[r] = state
        self.block_slots[r] = slot

        block = spec.BeaconBlock(
            slot=spec.Slot(slot),
            proposer_index=spec.ValidatorIndex(event["proposer"]),
            parent_root=parent_root,
        )
        bid = block.body.signed_execution_payload_bid.message
        bid.block_hash = block_hash(spec, event["bid_block_hash"])
        bid.parent_block_hash = block_hash(spec, event["bid_parent_block_hash"])

        # `on_block` store mutations, minus `state_transition` (see module docstring).
        head = spec.get_head(self.store)
        self.store.blocks[r] = block
        self.store.block_states[r] = state
        self.store.payload_timeliness_vote[r] = [None] * spec.PTC_SIZE
        self.store.payload_data_availability_vote[r] = [None] * spec.PTC_SIZE
        spec.record_block_timeliness(self.store, r)
        spec.update_proposer_boost_root(self.store, head.root, r)
        spec.update_checkpoints(
            self.store, state.current_justified_checkpoint, state.finalized_checkpoint
        )
        spec.compute_pulled_up_tip(self.store, r)
        self.ensure_justified_state()

    def ensure_justified_state(self):
        justified = self.store.justified_checkpoint
        if justified not in self.store.checkpoint_states:
            self.spec.store_target_checkpoint_state(self.store, justified)

    def replay_attestation(self, event):
        spec = self.spec
        attestation = spec.Attestation(
            data=spec.AttestationData(
                slot=spec.Slot(event["slot"]),
                index=spec.CommitteeIndex(event["index"]),
                beacon_block_root=root(spec, event["beacon_block_root"]),
                source=self.store.justified_checkpoint,
                target=spec.Checkpoint(
                    epoch=spec.Epoch(event["target_epoch"]),
                    root=root(spec, event["target_root"]),
                ),
            )
        )
        self.tick_to(self.seconds_at(event["delivery_slot"], False))
        try:
            self.apply_attestation(attestation, event["validator"])
        except AssertionError:
            if attestation.data.slot + 1 > spec.get_current_slot(self.store):
                self.pending.append((attestation, event["validator"]))
            else:
                self.mismatches.append(f"attestation rejected by pyspec: {event}")

    def replay_query(self, event):
        spec = self.spec
        self.tick_to(self.seconds_at(event["slot"], False))
        self.ensure_justified_state()
        head = spec.get_head(self.store)
        got_head = f"0x{bytes(head.root).hex()}"
        if got_head != event["head_root"]:
            self.mismatches.append(
                f"head at slot {event['slot']}: pyspec {got_head} != lighthouse {event['head_root']}"
            )
        justified = self.store.justified_checkpoint
        got_justified = (int(justified.epoch), f"0x{bytes(justified.root).hex()}")
        expected_justified = (event["justified_epoch"], event["justified_root"])
        if got_justified != expected_justified:
            self.mismatches.append(
                f"justified at slot {event['slot']}: pyspec {got_justified} != "
                f"lighthouse {expected_justified}"
            )

    def run(self, events):
        for event in events:
            kind = event["type"]
            if kind == "block":
                self.replay_block(event)
            elif kind == "reveal":
                self.store.payloads[root(self.spec, event["root"])] = (
                    self.spec.ExecutionPayloadEnvelope()
                )
            elif kind == "attestation":
                self.replay_attestation(event)
            elif kind == "attester_slashing":
                self.store.equivocating_indices.add(
                    self.spec.ValidatorIndex(event["validator"])
                )
            elif kind == "query":
                self.replay_query(event)
            else:
                raise ValueError(f"unknown event type {kind}")
        return self.mismatches


def main():
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <handler_sequences.jsonl>")

    spec = load_spec()

    with open(sys.argv[1]) as f:
        lines = [json.loads(line) for line in f if line.strip()]

    header, scenarios = lines[0], lines[1:]
    if header["format_version"] != FORMAT_VERSION:
        sys.exit(f"unsupported format version {header['format_version']}")
    if header["spec_version"] != SPEC_VERSION:
        sys.exit(f"scenarios exported for {header['spec_version']}, script pins {SPEC_VERSION}")
    assert header["kind"] == "handler_sequences"
    assert header["ptc_size"] == spec.PTC_SIZE
    assert header["slots_per_epoch"] == spec.SLOTS_PER_EPOCH
    assert header["proposer_score_boost"] == spec.config.PROPOSER_SCORE_BOOST

    total_mismatches = 0
    queries = 0
    for index, scenario in enumerate(scenarios):
        queries += sum(1 for e in scenario["events"] if e["type"] == "query")
        mismatches = Replay(spec, scenario).run(scenario["events"])
        for mismatch in mismatches:
            total_mismatches += 1
            print(f"MISMATCH in scenario {index} ({scenario['family']}): {mismatch}")
            print(f"  {json.dumps(scenario['events'])}")

    print(
        f"certified {len(scenarios)} handler sequences ({queries} queries) against "
        f"pyspec {SPEC_VERSION}: {total_mismatches} mismatches"
    )
    sys.exit(1 if total_mismatches else 0)


if __name__ == "__main__":
    main()
