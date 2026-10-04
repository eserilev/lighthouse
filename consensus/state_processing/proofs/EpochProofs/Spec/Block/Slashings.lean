import EpochProofs.Spec.Helpers
import EpochProofs.Spec.RegistryUpdates
import EpochProofs.Spec.Slashings
import EpochProofs.Spec.TotalActiveBalance

/-!
# Reference: proposer and attester slashings

Transcribed from `specs/phase0`, `specs/electra` and `specs/gloas/beacon-chain.md`
(v1.7.0-beta.2). Each definition quotes the pyspec of its latest fork. Hashes and BLS come from
the `Oracle`.
-/

namespace EpochProofs.Spec

/-- ```python
def slash_validator(
    state: BeaconState,
    slashed_index: ValidatorIndex,
    whistleblower_index: Optional[ValidatorIndex] = None,
) -> None:
    """
    Slash the validator with index ``slashed_index``.
    """
    epoch = get_current_epoch(state)
    initiate_validator_exit(state, slashed_index)
    validator = state.validators[slashed_index]
    validator.slashed = Boolean(True)
    validator.withdrawable_epoch = max(
        validator.withdrawable_epoch, epoch + EPOCHS_PER_SLASHINGS_VECTOR
    )
    state.slashings[epoch % EPOCHS_PER_SLASHINGS_VECTOR] += validator.effective_balance
    # [Modified in Electra:EIP7251]
    slashing_penalty = validator.effective_balance // MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA
    decrease_balance(state, slashed_index, slashing_penalty)

    # Apply proposer and whistleblower rewards
    proposer_index = get_beacon_proposer_index(state)
    if whistleblower_index is None:
        whistleblower_index = proposer_index
    # [Modified in Electra:EIP7251]
    whistleblower_reward = validator.effective_balance // WHISTLEBLOWER_REWARD_QUOTIENT_ELECTRA
    proposer_reward = whistleblower_reward * PROPOSER_WEIGHT // WEIGHT_DENOMINATOR
    increase_balance(state, proposer_index, proposer_reward)
    increase_balance(state, whistleblower_index, whistleblower_reward - proposer_reward)
```

`initiate_validator_exit` reads `get_total_active_balance(state)` only for a validator with no
exit. The reference makes the same early-return check first, and computes the total only on
the other path. So it raises exactly when Python raises. `slash_validator_initiate_eq` shows
that this step equals `initiate_validator_exit`. -/
def slash_validator (p : Preset) (state : BeaconState) (slashed_index : ValidatorIndex)
    (whistleblower_index : Option ValidatorIndex := none) : SpecM BeaconState := do
  let epoch ← get_current_epoch p state
  let state ← do
    let validator ← listGet state.validators slashed_index
    if validator.exit_epoch != FAR_FUTURE_EPOCH then pure state
    else initiate_validator_exit p (← get_total_active_balance p state) state slashed_index
  let validator ← listGet state.validators slashed_index
  let validator := { validator with slashed := true }
  let validator := { validator with
    withdrawable_epoch :=
      max validator.withdrawable_epoch (← uint64Add epoch p.EPOCHS_PER_SLASHINGS_VECTOR) }
  let state := { state with validators := ← listSet state.validators slashed_index validator }
  let slashings_index ← uint64Mod epoch p.EPOCHS_PER_SLASHINGS_VECTOR
  let state := { state with
    slashings := ← listSet state.slashings slashings_index
      (← uint64Add (← listGet state.slashings slashings_index) validator.effective_balance) }
  let slashing_penalty ←
    uint64Div validator.effective_balance p.MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA
  let state := { state with
    balances := ← decrease_balance state.balances slashed_index slashing_penalty }

  let proposer_index ← get_beacon_proposer_index p state
  let whistleblower_index := match whistleblower_index with
    | none => proposer_index
    | some i => i
  let whistleblower_reward ←
    uint64Div validator.effective_balance p.WHISTLEBLOWER_REWARD_QUOTIENT_ELECTRA
  let proposer_reward ←
    uint64Div (← uint64Mul whistleblower_reward PROPOSER_WEIGHT) WEIGHT_DENOMINATOR
  let state := { state with
    balances := ← increase_balance state.balances proposer_index proposer_reward }
  let state := { state with
    balances := ← increase_balance state.balances whistleblower_index
      (← uint64Sub whistleblower_reward proposer_reward) }
  pure state

/-- ```python
def is_slashable_validator(validator: Validator, epoch: Epoch) -> bool:
    """
    Check if ``validator`` is slashable.
    """
    return (not validator.slashed) and (
        validator.activation_epoch <= epoch < validator.withdrawable_epoch
    )
``` -/
def is_slashable_validator (validator : Validator) (epoch : Epoch) : Bool :=
  !validator.slashed &&
    (validator.activation_epoch ≤ epoch && epoch < validator.withdrawable_epoch)

/-- ```python
def is_slashable_attestation_data(data_1: AttestationData, data_2: AttestationData) -> bool:
    """
    Check if ``data_1`` and ``data_2`` are slashable according to Casper FFG rules.
    """
    return (
        # Double vote
        (data_1 != data_2 and data_1.target.epoch == data_2.target.epoch)
        or
        # Surround vote
        (data_1.source.epoch < data_2.source.epoch and data_2.target.epoch < data_1.target.epoch)
    )
``` -/
def is_slashable_attestation_data (data_1 data_2 : AttestationData) : Bool :=
  (data_1 != data_2 && data_1.target.epoch == data_2.target.epoch)
    || (data_1.source.epoch < data_2.source.epoch && data_2.target.epoch < data_1.target.epoch)

/-- ```python
def is_valid_indexed_attestation(
    state: BeaconState, indexed_attestation: IndexedAttestation
) -> bool:
    """
    Check if ``indexed_attestation`` is not empty, has sorted and unique indices and has a valid aggregate signature.
    """
    # Verify indices are sorted and unique
    indices = indexed_attestation.attesting_indices
    if (
        len(indices) == 0
        # [New in Gloas:EIP7688]
        or len(indices) > MAX_VALIDATORS_PER_COMMITTEE * MAX_COMMITTEES_PER_SLOT
        or list(indices) != sorted(set(indices))
    ):
        return False
    # Verify aggregate signature
    pubkeys = [state.validators[i].pubkey for i in indices]
    domain = get_domain(state, DOMAIN_BEACON_ATTESTER, indexed_attestation.data.target.epoch)
    signing_root = compute_signing_root(indexed_attestation.data, domain)
    return bls.FastAggregateVerify(pubkeys, signing_root, indexed_attestation.signature)
```

`sorted(set(indices))` is `indices.eraseDups.mergeSort`. -/
def is_valid_indexed_attestation (p : Preset) (o : Oracle) (state : BeaconState)
    (indexed_attestation : IndexedAttestation) : SpecM Bool := do
  let indices := indexed_attestation.attesting_indices
  if indices.length = 0
      || indices.length > p.MAX_VALIDATORS_PER_COMMITTEE * p.MAX_COMMITTEES_PER_SLOT
      || indices != indices.eraseDups.mergeSort then
    return false
  let pubkeys ← indices.mapM fun i => do pure (← listGet state.validators i).pubkey
  let domain ←
    get_domain p o state DOMAIN_BEACON_ATTESTER (some indexed_attestation.data.target.epoch)
  let signing_root :=
    compute_signing_root o (o.hash_tree_root_AttestationData indexed_attestation.data) domain
  pure (o.bls_FastAggregateVerify pubkeys signing_root indexed_attestation.signature)

/-- ```python
def process_proposer_slashing(state: BeaconState, proposer_slashing: ProposerSlashing) -> None:
    header_1 = proposer_slashing.signed_header_1.message
    header_2 = proposer_slashing.signed_header_2.message

    # Verify header slots match
    assert header_1.slot == header_2.slot
    # Verify header proposer indices match
    assert header_1.proposer_index == header_2.proposer_index
    # Verify the headers are different
    assert header_1 != header_2
    # Verify the proposer is slashable
    proposer = state.validators[header_1.proposer_index]
    assert is_slashable_validator(proposer, get_current_epoch(state))
    # Verify signatures
    for signed_header in (proposer_slashing.signed_header_1, proposer_slashing.signed_header_2):
        domain = get_domain(
            state, DOMAIN_BEACON_PROPOSER, compute_epoch_at_slot(signed_header.message.slot)
        )
        signing_root = compute_signing_root(signed_header.message, domain)
        assert bls.Verify(proposer.pubkey, signing_root, signed_header.signature)

    # [New in Gloas:EIP7732]
    # Remove the BuilderPendingPayment corresponding to this proposal if it is
    # still in the 2-epoch window. Only clear it when the slashed validator is
    # the proposer associated with the payment; otherwise an unrelated same-slot
    # equivocation could grief an honest proposer's payment.
    slot = header_1.slot
    proposal_epoch = compute_epoch_at_slot(slot)
    if proposal_epoch == get_current_epoch(state):
        payment_index = SLOTS_PER_EPOCH + slot % SLOTS_PER_EPOCH
        payment = state.builder_pending_payments[payment_index]
        if payment.proposer_index == header_1.proposer_index:
            state.builder_pending_payments[payment_index] = BuilderPendingPayment.empty()
    elif proposal_epoch == get_previous_epoch(state):
        payment_index = slot % SLOTS_PER_EPOCH
        payment = state.builder_pending_payments[payment_index]
        if payment.proposer_index == header_1.proposer_index:
            state.builder_pending_payments[payment_index] = BuilderPendingPayment.empty()

    slash_validator(state, header_1.proposer_index)
``` -/
def process_proposer_slashing (p : Preset) (o : Oracle) (state : BeaconState)
    (proposer_slashing : ProposerSlashing) : SpecM BeaconState := do
  let header_1 := proposer_slashing.signed_header_1.message
  let header_2 := proposer_slashing.signed_header_2.message

  if ¬ header_1.slot = header_2.slot then throw .assertionFailed
  if ¬ header_1.proposer_index = header_2.proposer_index then throw .assertionFailed
  if ¬ header_1 ≠ header_2 then throw .assertionFailed
  let proposer ← listGet state.validators header_1.proposer_index
  if ¬ is_slashable_validator proposer (← get_current_epoch p state) then throw .assertionFailed
  for signed_header in [proposer_slashing.signed_header_1, proposer_slashing.signed_header_2] do
    let domain ← get_domain p o state DOMAIN_BEACON_PROPOSER
      (some (← compute_epoch_at_slot p signed_header.message.slot))
    let signing_root :=
      compute_signing_root o (o.hash_tree_root_BeaconBlockHeader signed_header.message) domain
    if ¬ o.bls_Verify proposer.pubkey signing_root signed_header.signature then
      throw .assertionFailed

  let slot := header_1.slot
  let proposal_epoch ← compute_epoch_at_slot p slot
  let mut state := state
  if proposal_epoch = (← get_current_epoch p state) then
    let payment_index ← uint64Add p.SLOTS_PER_EPOCH (← uint64Mod slot p.SLOTS_PER_EPOCH)
    let payment ← listGet state.builder_pending_payments payment_index
    if payment.proposer_index = header_1.proposer_index then
      state := { state with
        builder_pending_payments :=
          ← listSet state.builder_pending_payments payment_index .empty }
  else
    if proposal_epoch = (← get_previous_epoch p state) then
      let payment_index ← uint64Mod slot p.SLOTS_PER_EPOCH
      let payment ← listGet state.builder_pending_payments payment_index
      if payment.proposer_index = header_1.proposer_index then
        state := { state with
          builder_pending_payments :=
            ← listSet state.builder_pending_payments payment_index .empty }

  slash_validator p state header_1.proposer_index

/-- ```python
def process_attester_slashing(state: BeaconState, attester_slashing: AttesterSlashing) -> None:
    attestation_1 = attester_slashing.attestation_1
    attestation_2 = attester_slashing.attestation_2
    assert is_slashable_attestation_data(attestation_1.data, attestation_2.data)
    assert is_valid_indexed_attestation(state, attestation_1)
    assert is_valid_indexed_attestation(state, attestation_2)

    slashed_any = False
    indices = set(attestation_1.attesting_indices).intersection(attestation_2.attesting_indices)
    for index in sorted(indices):
        if is_slashable_validator(state.validators[index], get_current_epoch(state)):
            slash_validator(state, index)
            slashed_any = True
    assert slashed_any
```

`sorted(indices)` is the sorted list of the indices in both attestations, without repeats. -/
def process_attester_slashing (p : Preset) (o : Oracle) (state : BeaconState)
    (attester_slashing : AttesterSlashing) : SpecM BeaconState := do
  let attestation_1 := attester_slashing.attestation_1
  let attestation_2 := attester_slashing.attestation_2
  if ¬ is_slashable_attestation_data attestation_1.data attestation_2.data then
    throw .assertionFailed
  if ¬ (← is_valid_indexed_attestation p o state attestation_1) then
    throw .assertionFailed
  if ¬ (← is_valid_indexed_attestation p o state attestation_2) then
    throw .assertionFailed

  let mut slashed_any := false
  let indices := (attestation_1.attesting_indices.filter
    (· ∈ attestation_2.attesting_indices)).eraseDups
  let mut state := state
  for index in indices.mergeSort do
    if is_slashable_validator (← listGet state.validators index) (← get_current_epoch p state) then
      state ← slash_validator p state index
      slashed_any := true
  if ¬ slashed_any then throw .assertionFailed
  pure state

end EpochProofs.Spec
