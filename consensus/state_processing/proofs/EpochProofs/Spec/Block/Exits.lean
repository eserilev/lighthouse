import EpochProofs.Spec.Block.ExecutionRequests

/-!
# Reference: voluntary exits and BLS to execution changes

Transcribed from `specs/capella` and `specs/electra/beacon-chain.md` (v1.7.0-beta.2). Gloas
does not change these functions. Each definition quotes its pyspec. Hashes and BLS come from
the `Oracle`. `get_pending_balance_to_withdraw` comes from `Spec/Block/ExecutionRequests.lean`.
-/

namespace EpochProofs.Spec

/-- ```python
def process_voluntary_exit(state: BeaconState, signed_voluntary_exit: SignedVoluntaryExit) -> None:
    voluntary_exit = signed_voluntary_exit.message
    validator = state.validators[voluntary_exit.validator_index]
    # Verify the validator is active
    assert is_active_validator(validator, get_current_epoch(state))
    # Verify exit has not been initiated
    assert validator.exit_epoch == FAR_FUTURE_EPOCH
    # Exits must specify an epoch when they become valid; they are not valid before then
    assert get_current_epoch(state) >= voluntary_exit.epoch
    # Verify the validator has been active long enough
    assert get_current_epoch(state) >= validator.activation_epoch + SHARD_COMMITTEE_PERIOD
    # [New in Electra:EIP7251]
    # Only exit validator if it has no pending withdrawals in the queue
    assert get_pending_balance_to_withdraw(state, voluntary_exit.validator_index) == 0
    # Verify signature
    domain = compute_domain(
        DOMAIN_VOLUNTARY_EXIT, CAPELLA_FORK_VERSION, state.genesis_validators_root
    )
    signing_root = compute_signing_root(voluntary_exit, domain)
    assert bls.Verify(validator.pubkey, signing_root, signed_voluntary_exit.signature)
    # Initiate exit
    initiate_validator_exit(state, voluntary_exit.validator_index)
```

The asserts above require `validator.exit_epoch == FAR_FUTURE_EPOCH`. So Python always reads
`get_total_active_balance(state)` in `initiate_validator_exit`, and this reference reads it on
the same path. -/
def process_voluntary_exit (p : Preset) (o : Oracle) (state : BeaconState)
    (signed_voluntary_exit : SignedVoluntaryExit) : SpecM BeaconState := do
  let voluntary_exit := signed_voluntary_exit.message
  let validator ← listGet state.validators voluntary_exit.validator_index
  if ¬ is_active_validator validator (← get_current_epoch p state) then throw .assertionFailed
  if ¬ validator.exit_epoch = FAR_FUTURE_EPOCH then throw .assertionFailed
  if ¬ (← get_current_epoch p state) ≥ voluntary_exit.epoch then throw .assertionFailed
  if ¬ (← get_current_epoch p state) ≥
      (← uint64Add validator.activation_epoch p.SHARD_COMMITTEE_PERIOD) then
    throw .assertionFailed
  if ¬ (← get_pending_balance_to_withdraw state voluntary_exit.validator_index) = 0 then
    throw .assertionFailed
  let domain := compute_domain p o DOMAIN_VOLUNTARY_EXIT (some p.CAPELLA_FORK_VERSION)
    (some state.genesis_validators_root)
  let signing_root :=
    compute_signing_root o (o.hash_tree_root_VoluntaryExit voluntary_exit) domain
  if ¬ o.bls_Verify validator.pubkey signing_root signed_voluntary_exit.signature then
    throw .assertionFailed
  initiate_validator_exit p (← get_total_active_balance p state) state
    voluntary_exit.validator_index

/-- ```python
def process_bls_to_execution_change(
    state: BeaconState, signed_address_change: SignedBLSToExecutionChange
) -> None:
    address_change = signed_address_change.message

    assert address_change.validator_index < len(state.validators)

    validator = state.validators[address_change.validator_index]

    assert validator.withdrawal_credentials[:1] == BLS_WITHDRAWAL_PREFIX
    assert validator.withdrawal_credentials[1:] == sha256(address_change.from_bls_pubkey)[1:]

    # Fork-agnostic domain since address changes are valid across forks
    domain = compute_domain(
        DOMAIN_BLS_TO_EXECUTION_CHANGE, genesis_validators_root=state.genesis_validators_root
    )
    signing_root = compute_signing_root(address_change, domain)
    assert bls.Verify(address_change.from_bls_pubkey, signing_root, signed_address_change.signature)

    validator.withdrawal_credentials = Bytes32(
        ETH1_ADDRESS_WITHDRAWAL_PREFIX + b"\x00" * 11 + address_change.to_execution_address
    )
``` -/
def process_bls_to_execution_change (p : Preset) (o : Oracle) (state : BeaconState)
    (signed_address_change : SignedBLSToExecutionChange) : SpecM BeaconState := do
  let address_change := signed_address_change.message

  if ¬ address_change.validator_index < state.validators.length then throw .assertionFailed

  let validator ← listGet state.validators address_change.validator_index

  if ¬ validator.withdrawal_credentials.toList.take 1 = [BLS_WITHDRAWAL_PREFIX] then
    throw .assertionFailed
  if ¬ validator.withdrawal_credentials.toList.drop 1 =
      (o.hash address_change.from_bls_pubkey.toList).toList.drop 1 then
    throw .assertionFailed

  let domain := compute_domain p o DOMAIN_BLS_TO_EXECUTION_CHANGE
    (genesis_validators_root := some state.genesis_validators_root)
  let signing_root :=
    compute_signing_root o (o.hash_tree_root_BLSToExecutionChange address_change) domain
  if ¬ o.bls_Verify address_change.from_bls_pubkey signing_root signed_address_change.signature
      then
    throw .assertionFailed

  let validator := { validator with
    withdrawal_credentials :=
      ((#v[ETH1_ADDRESS_WITHDRAWAL_PREFIX] ++ Vector.replicate 11 (0 : UInt8)) ++
        address_change.to_execution_address).cast (by decide) }
  pure { state with
    validators := ← listSet state.validators address_change.validator_index validator }

end EpochProofs.Spec
