import EpochProofs.Spec.Block.ExecutionRequests

/-!
# Reference: `process_sync_aggregate`

Transcribed from `specs/phase0/beacon-chain.md`, `specs/altair/beacon-chain.md` and
`specs/altair/bls.md` (v1.7.0-beta.2). Later forks do not change these functions. Each
definition quotes its pyspec. BLS comes from the `Oracle`.
-/

namespace EpochProofs.Spec

/-- ```python
def get_set_bit_count(bits: Sequence[Boolean]) -> Uint64:
    """
    Return the number of bits that are set in ``bits``.
    """
    return Uint64(sum(1 for bit in bits if bit))
``` -/
def get_set_bit_count (bits : List Bool) : Uint64 :=
  (bits.filter id).length

/-- ```python
def eth_aggregate_pubkeys(pubkeys: Sequence[BLSPubkey]) -> BLSPubkey:
    """
    Return the aggregate public key for the public keys in ``pubkeys``.

    Note: the ``+`` operation should be interpreted as elliptic curve point addition, which takes as input
    elliptic curve points that must be decoded from the input ``BLSPubkey``s.
    This implementation is for demonstrative purposes only and ignores encoding/decoding concerns.
    Refer to the BLS signature draft standard for more information.
    """
    assert len(pubkeys) > 0
    # Ensure that the given inputs are valid pubkeys
    assert all(bls.KeyValidate(pubkey) for pubkey in pubkeys)

    result = pubkeys[0].copy()
    for pubkey in pubkeys[1:]:
        result += pubkey
    return result
```

The point sum of the keys is `bls.AggregatePKs`. -/
def eth_aggregate_pubkeys (o : Oracle) (pubkeys : List BLSPubkey) : SpecM BLSPubkey := do
  if ¬ pubkeys.length > 0 then throw .assertionFailed
  if ¬ pubkeys.all o.bls_KeyValidate then throw .assertionFailed
  pure (o.bls_AggregatePKs pubkeys)

/-- ```python
def eth_fast_aggregate_verify(
    pubkeys: Sequence[BLSPubkey], message: Bytes32, signature: BLSSignature
) -> bool:
    """
    Wrapper to ``bls.FastAggregateVerify`` accepting the ``G2_POINT_AT_INFINITY`` signature when ``pubkeys`` is empty.
    """
    if len(pubkeys) == 0 and signature == G2_POINT_AT_INFINITY:
        return True
    return bls.FastAggregateVerify(pubkeys, message, signature)
``` -/
def eth_fast_aggregate_verify (o : Oracle) (pubkeys : List BLSPubkey) (message : Bytes32)
    (signature : BLSSignature) : Bool :=
  if pubkeys.length == 0 && signature == G2_POINT_AT_INFINITY then true
  else o.bls_FastAggregateVerify pubkeys message signature

/-- `l.index(a)` in Python: the first position of `a` in `l`. It raises when `a` is not in
`l`. -/
def listIndexOf {α : Type} [BEq α] (l : List α) (a : α) : SpecM Nat :=
  match l.findIdx? (· == a) with
  | some i => pure i
  | none => throw .indexOutOfRange

/-- The `for` loop at the end of `process_sync_aggregate`, over
`zip(committee_indices, sync_aggregate.sync_committee_bits)`.

`get_beacon_proposer_index` reads only `slot` and `proposer_lookahead`. The loop does not
write them, so the loop reads them from `state`. -/
def process_sync_aggregate_loop (p : Preset) (state : BeaconState)
    (participant_reward proposer_reward : Gwei) :
    List (ValidatorIndex × Bool) → List Gwei → SpecM (List Gwei)
  | [], balances => pure balances
  | (participant_index, participation_bit) :: rest, balances => do
    let balances ←
      if participation_bit then do
        let balances ← increase_balance balances participant_index participant_reward
        let proposer_index ← get_beacon_proposer_index p state
        increase_balance balances proposer_index proposer_reward
      else
        decrease_balance balances participant_index participant_reward
    process_sync_aggregate_loop p state participant_reward proposer_reward rest balances

/-- ```python
def process_sync_aggregate(state: BeaconState, sync_aggregate: SyncAggregate) -> None:
    # Verify sync committee aggregate signature signing over the previous slot block root
    committee_pubkeys = state.current_sync_committee.pubkeys
    committee_bits = sync_aggregate.sync_committee_bits
    if get_set_bit_count(committee_bits) == SYNC_COMMITTEE_SIZE:
        # All members participated - use precomputed aggregate key
        participant_pubkeys = [state.current_sync_committee.aggregate_pubkey]
    elif get_set_bit_count(committee_bits) > SYNC_COMMITTEE_SIZE // 2:
        # More than half participated - subtract non-participant keys.
        # First determine nonparticipating members
        non_participant_pubkeys = [
            pubkey for pubkey, bit in zip(committee_pubkeys, committee_bits, strict=True) if not bit
        ]
        # Compute aggregate of non-participants
        non_participant_aggregate = eth_aggregate_pubkeys(non_participant_pubkeys)
        # Subtract non-participants from the full aggregate
        # This is equivalent to: aggregate_pubkey + (-non_participant_aggregate)
        participant_pubkey = bls.add(
            bls.bytes48_to_G1(state.current_sync_committee.aggregate_pubkey),
            bls.neg(bls.bytes48_to_G1(non_participant_aggregate)),
        )
        participant_pubkeys = [BLSPubkey(bls.G1_to_bytes48(participant_pubkey))]
    else:
        # Less than half participated - aggregate participant keys
        participant_pubkeys = [
            pubkey
            for pubkey, bit in zip(
                committee_pubkeys, sync_aggregate.sync_committee_bits, strict=True
            )
            if bit
        ]
    previous_slot = saturating_sub(state.slot, 1)
    domain = get_domain(state, DOMAIN_SYNC_COMMITTEE, compute_epoch_at_slot(previous_slot))
    signing_root = compute_signing_root(get_block_root_at_slot(state, previous_slot), domain)
    # Note: eth_fast_aggregate_verify works with a singleton list containing an aggregated key
    assert eth_fast_aggregate_verify(
        participant_pubkeys, signing_root, sync_aggregate.sync_committee_signature
    )

    # Compute participant and proposer rewards
    total_active_increments = get_total_active_balance(state) // EFFECTIVE_BALANCE_INCREMENT
    total_base_rewards = get_base_reward_per_increment(state) * total_active_increments
    max_participant_rewards = (
        total_base_rewards * SYNC_REWARD_WEIGHT // WEIGHT_DENOMINATOR // Uint64(SLOTS_PER_EPOCH)
    )
    participant_reward = max_participant_rewards // SYNC_COMMITTEE_SIZE
    proposer_reward = participant_reward * PROPOSER_WEIGHT // (WEIGHT_DENOMINATOR - PROPOSER_WEIGHT)

    # Apply participant and proposer rewards
    all_pubkeys = [v.pubkey for v in state.validators]
    committee_indices = [
        ValidatorIndex(all_pubkeys.index(pubkey)) for pubkey in state.current_sync_committee.pubkeys
    ]
    for participant_index, participation_bit in zip(
        committee_indices, sync_aggregate.sync_committee_bits, strict=True
    ):
        if participation_bit:
            increase_balance(state, participant_index, participant_reward)
            increase_balance(state, get_beacon_proposer_index(state), proposer_reward)
        else:
            decrease_balance(state, participant_index, participant_reward)
```

`zip(..., strict=True)` raises when the lengths differ. The point subtraction is
`bls_SubtractPK`. Each top-level `assert` has an explicit `else`, so the rest of the function
is one bind chain. -/
def process_sync_aggregate (p : Preset) (o : Oracle) (state : BeaconState)
    (sync_aggregate : SyncAggregate) : SpecM BeaconState := do
  let committee_pubkeys := state.current_sync_committee.pubkeys
  let committee_bits := sync_aggregate.sync_committee_bits
  let participant_pubkeys ← (do
    if get_set_bit_count committee_bits == p.SYNC_COMMITTEE_SIZE then
      pure [state.current_sync_committee.aggregate_pubkey]
    else if get_set_bit_count committee_bits > (← uint64Div p.SYNC_COMMITTEE_SIZE 2) then do
      if committee_pubkeys.length ≠ committee_bits.length then throw .assertionFailed
      let non_participant_pubkeys :=
        ((committee_pubkeys.zip committee_bits).filter fun (_, bit) => !bit).map (·.1)
      let non_participant_aggregate ← eth_aggregate_pubkeys o non_participant_pubkeys
      let participant_pubkey :=
        o.bls_SubtractPK state.current_sync_committee.aggregate_pubkey non_participant_aggregate
      pure [participant_pubkey]
    else do
      if committee_pubkeys.length ≠ sync_aggregate.sync_committee_bits.length then
        throw .assertionFailed
      pure (((committee_pubkeys.zip sync_aggregate.sync_committee_bits).filter
        fun (_, bit) => bit).map (·.1)))
  let previous_slot := saturating_sub state.slot 1
  let domain ← get_domain p o state DOMAIN_SYNC_COMMITTEE
    (some (← compute_epoch_at_slot p previous_slot))
  let signing_root := compute_signing_root o (← get_block_root_at_slot p state previous_slot) domain
  if ¬ eth_fast_aggregate_verify o participant_pubkeys signing_root
      sync_aggregate.sync_committee_signature then
    throw .assertionFailed
  else do

  -- Compute participant and proposer rewards
  let total_active_increments ←
    uint64Div (← get_total_active_balance p state) p.EFFECTIVE_BALANCE_INCREMENT
  let total_base_rewards ←
    uint64Mul (← get_base_reward_per_increment p (← get_total_active_balance p state))
      total_active_increments
  let max_participant_rewards ←
    uint64Div (← uint64Div (← uint64Mul total_base_rewards SYNC_REWARD_WEIGHT) WEIGHT_DENOMINATOR)
      p.SLOTS_PER_EPOCH
  let participant_reward ← uint64Div max_participant_rewards p.SYNC_COMMITTEE_SIZE
  let proposer_reward ← uint64Div (← uint64Mul participant_reward PROPOSER_WEIGHT)
    (← uint64Sub WEIGHT_DENOMINATOR PROPOSER_WEIGHT)

  -- Apply participant and proposer rewards
  let all_pubkeys := state.validators.map (·.pubkey)
  let committee_indices ←
    state.current_sync_committee.pubkeys.mapM fun pubkey => listIndexOf all_pubkeys pubkey
  if committee_indices.length ≠ sync_aggregate.sync_committee_bits.length then
    throw .assertionFailed
  else do
  let balances ← process_sync_aggregate_loop p state participant_reward proposer_reward
    (committee_indices.zip sync_aggregate.sync_committee_bits) state.balances
  pure { state with balances }

end EpochProofs.Spec
