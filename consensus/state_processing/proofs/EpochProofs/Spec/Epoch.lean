import EpochProofs.Spec.JustificationFinalization
import EpochProofs.Spec.InactivityUpdates
import EpochProofs.Spec.RewardsAndPenalties
import EpochProofs.Spec.RegistryUpdates
import EpochProofs.Spec.Slashings
import EpochProofs.Spec.EpochRest
import EpochProofs.Spec.PendingDeposits
import EpochProofs.Spec.PendingConsolidations
import EpochProofs.Spec.BuilderPendingPayments
import EpochProofs.Spec.EffectiveBalanceUpdates
import EpochProofs.Spec.EpochCommittees
import EpochProofs.Spec.Block.ExecutionRequests

/-!
# Reference: `process_epoch` and `apply_pending_deposit`

Transcribed from `specs/phase0`, `specs/altair`, `specs/electra` and `specs/gloas`
`beacon-chain.md` (v1.7.0-beta.2). Each definition quotes the pyspec of its latest fork.
`process_epoch` runs the epoch steps of the other files in pyspec order.
-/

namespace EpochProofs.Spec

/-- ```python
def get_index_for_new_validator(state: BeaconState) -> ValidatorIndex:
    return ValidatorIndex(len(state.validators))
``` -/
def get_index_for_new_validator (state : BeaconState) : ValidatorIndex :=
  state.validators.length

/-- ```python
def get_validator_from_deposit(
    pubkey: BLSPubkey, withdrawal_credentials: Bytes32, amount: Gwei
) -> Validator:
    validator = Validator(
        pubkey=pubkey,
        withdrawal_credentials=withdrawal_credentials,
        effective_balance=Gwei(0),
        slashed=Boolean(False),
        activation_eligibility_epoch=FAR_FUTURE_EPOCH,
        activation_epoch=FAR_FUTURE_EPOCH,
        exit_epoch=FAR_FUTURE_EPOCH,
        withdrawable_epoch=FAR_FUTURE_EPOCH,
    )

    # [Modified in Electra:EIP7251]
    max_effective_balance = get_max_effective_balance(validator)
    validator.effective_balance = min(
        amount - amount % EFFECTIVE_BALANCE_INCREMENT, max_effective_balance
    )

    return validator
``` -/
def get_validator_from_deposit (p : Preset) (pubkey : BLSPubkey) (withdrawal_credentials : Bytes32)
    (amount : Gwei) : SpecM Validator := do
  let validator : Validator := {
    pubkey
    withdrawal_credentials
    effective_balance := 0
    slashed := false
    activation_eligibility_epoch := FAR_FUTURE_EPOCH
    activation_epoch := FAR_FUTURE_EPOCH
    exit_epoch := FAR_FUTURE_EPOCH
    withdrawable_epoch := FAR_FUTURE_EPOCH }

  let max_effective_balance := get_max_effective_balance p validator
  let validator := { validator with
    effective_balance :=
      min (← uint64Sub amount (← uint64Mod amount p.EFFECTIVE_BALANCE_INCREMENT))
        max_effective_balance }

  pure validator

/-- ```python
def add_validator_to_registry(
    state: BeaconState, pubkey: BLSPubkey, withdrawal_credentials: Bytes32, amount: Gwei
) -> None:
    index = get_index_for_new_validator(state)
    # [Modified in Electra:EIP7251]
    validator = get_validator_from_deposit(pubkey, withdrawal_credentials, amount)
    set_or_append_list(state.validators, index, validator)
    set_or_append_list(state.balances, index, amount)
    set_or_append_list(state.previous_epoch_participation, index, ParticipationFlags(0b0000_0000))
    set_or_append_list(state.current_epoch_participation, index, ParticipationFlags(0b0000_0000))
    set_or_append_list(state.inactivity_scores, index, Uint64(0))
``` -/
def add_validator_to_registry (p : Preset) (state : BeaconState) (pubkey : BLSPubkey)
    (withdrawal_credentials : Bytes32) (amount : Gwei) : SpecM BeaconState := do
  let index := get_index_for_new_validator state
  let validator ← get_validator_from_deposit p pubkey withdrawal_credentials amount
  let state := { state with validators := ← set_or_append_list state.validators index validator }
  let state := { state with balances := ← set_or_append_list state.balances index amount }
  let state := { state with
    previous_epoch_participation :=
      ← set_or_append_list state.previous_epoch_participation index 0 }
  let state := { state with
    current_epoch_participation := ← set_or_append_list state.current_epoch_participation index 0 }
  pure { state with
    inactivity_scores := ← set_or_append_list state.inactivity_scores index 0 }

/-- ```python
def is_valid_deposit_signature(
    pubkey: BLSPubkey, withdrawal_credentials: Bytes32, amount: Gwei, signature: BLSSignature
) -> bool:
    deposit_message = DepositMessage(
        pubkey=pubkey,
        withdrawal_credentials=withdrawal_credentials,
        amount=amount,
    )
    # Fork-agnostic domain since deposits are valid across forks
    domain = compute_domain(DOMAIN_DEPOSIT)
    signing_root = compute_signing_root(deposit_message, domain)
    return bls.Verify(pubkey, signing_root, signature)
``` -/
def is_valid_deposit_signature (p : Preset) (o : Oracle) (pubkey : BLSPubkey)
    (withdrawal_credentials : Bytes32) (amount : Gwei) (signature : BLSSignature) : Bool :=
  let deposit_message : DepositMessage := { pubkey, withdrawal_credentials, amount }
  let domain := compute_domain p o DOMAIN_DEPOSIT
  let signing_root :=
    compute_signing_root o (o.hash_tree_root_DepositMessage deposit_message) domain
  o.bls_Verify pubkey signing_root signature

/-- ```python
def apply_pending_deposit(state: BeaconState, deposit: PendingDeposit) -> None:
    validator_pubkeys = [v.pubkey for v in state.validators]
    if deposit.pubkey not in validator_pubkeys:
        # Verify the deposit signature (proof of possession) which is not checked by the deposit contract
        if is_valid_deposit_signature(
            deposit.pubkey, deposit.withdrawal_credentials, deposit.amount, deposit.signature
        ):
            add_validator_to_registry(
                state, deposit.pubkey, deposit.withdrawal_credentials, deposit.amount
            )
    else:
        validator_index = ValidatorIndex(validator_pubkeys.index(deposit.pubkey))
        increase_balance(state, validator_index, deposit.amount)
```

Gloas does not change this function. Builder deposits go through
`process_builder_deposit_request`, not through the pending deposit queue. -/
def apply_pending_deposit (p : Preset) (o : Oracle) (state : BeaconState)
    (deposit : PendingDeposit) : SpecM BeaconState := do
  let validator_pubkeys := state.validators.map (·.pubkey)
  if deposit.pubkey ∉ validator_pubkeys then
    if is_valid_deposit_signature p o deposit.pubkey deposit.withdrawal_credentials
        deposit.amount deposit.signature then
      add_validator_to_registry p state deposit.pubkey deposit.withdrawal_credentials
        deposit.amount
    else
      pure state
  else
    let validator_index := validator_pubkeys.idxOf deposit.pubkey
    pure { state with balances := ← increase_balance state.balances validator_index deposit.amount }

/-- ```python
def process_epoch(state: BeaconState) -> None:
    process_justification_and_finalization(state)
    process_inactivity_updates(state)
    process_rewards_and_penalties(state)
    process_registry_updates(state)
    process_slashings(state)
    process_eth1_data_reset(state)
    # [Modified in Gloas:EIP8061]
    process_pending_deposits(state)
    process_pending_consolidations(state)
    # [New in Gloas:EIP7732]
    process_builder_pending_payments(state)
    process_effective_balance_updates(state)
    process_slashings_reset(state)
    process_randao_mixes_reset(state)
    process_historical_summaries_update(state)
    process_participation_flag_updates(state)
    process_sync_committee_updates(state)
    process_proposer_lookahead(state)
    # [New in Gloas:EIP7732]
    process_ptc_window(state)
```

Some steps here take `total_active_balance` as a parameter. pyspec calls
`get_total_active_balance(state)` inside the step, and only when the step needs it. Here the
value comes from the state right before the step. Inside these steps the active set does not
change, so the value is the same. The two differ only when the sum of the effective balances
overflows `Uint64`: then this function raises, and pyspec raises only when it calls
`get_total_active_balance`. -/
def process_epoch (p : Preset) (o : Oracle) (state : BeaconState) : SpecM BeaconState := do
  let state ← process_justification_and_finalization p state
  let state ← process_inactivity_updates p state
  let state ← process_rewards_and_penalties p (← get_total_active_balance p state) state
  let state ← process_registry_updates p (← get_total_active_balance p state) state
  let state ← process_slashings p (← get_total_active_balance p state) state
  let state ← process_eth1_data_reset p state
  let state ←
    process_pending_deposits p (← get_total_active_balance p state) (apply_pending_deposit p o)
      state
  let state ← process_pending_consolidations p state
  let state ← process_builder_pending_payments p (← get_total_active_balance p state) state
  let state ← process_effective_balance_updates p state
  let state ← process_slashings_reset p state
  let state ← process_randao_mixes_reset p state
  let state ← process_historical_summaries_update p o state
  let state := process_participation_flag_updates state
  let state ← process_sync_committee_updates p o state
  let state ← process_proposer_lookahead p o state
  process_ptc_window p o state

end EpochProofs.Spec
