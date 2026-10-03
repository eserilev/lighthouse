import EpochProofs.Spec.PendingDeposits

/-!
# Reference: `process_pending_consolidations`

Transcribed line by line from `specs/electra/beacon-chain.md` (v1.7.0-beta.2). Gloas does not
change it. Each definition quotes its pyspec.
-/

namespace EpochProofs.Spec

/-- ```python
def process_pending_consolidations(state: BeaconState) -> None:
    next_epoch = get_current_epoch(state) + 1
    next_pending_consolidation = 0
    for pending_consolidation in state.pending_consolidations:
        source_validator = state.validators[pending_consolidation.source_index]
        if source_validator.slashed:
            next_pending_consolidation += 1
            continue
        if source_validator.withdrawable_epoch > next_epoch:
            break

        # Calculate the consolidated balance
        source_effective_balance = min(
            state.balances[pending_consolidation.source_index], source_validator.effective_balance
        )

        # Move active balance to target. Excess balance is withdrawable.
        decrease_balance(state, pending_consolidation.source_index, source_effective_balance)
        increase_balance(state, pending_consolidation.target_index, source_effective_balance)
        next_pending_consolidation += 1

    state.pending_consolidations = state.pending_consolidations[next_pending_consolidation:]
``` -/
def process_pending_consolidations (p : Preset) (state : BeaconState) : SpecM BeaconState := do
  let next_epoch ← uint64Add (← get_current_epoch p state) 1
  let mut next_pending_consolidation := 0
  let mut state := state
  for pending_consolidation in state.pending_consolidations do
    let source_validator ← listGet state.validators pending_consolidation.source_index
    if source_validator.slashed then
      next_pending_consolidation := next_pending_consolidation + 1
      continue
    if source_validator.withdrawable_epoch > next_epoch then
      break

    let source_effective_balance :=
      min (← listGet state.balances pending_consolidation.source_index)
        source_validator.effective_balance

    let balances ← decrease_balance state.balances pending_consolidation.source_index
      source_effective_balance
    state := { state with balances }
    let balances ← increase_balance state.balances pending_consolidation.target_index
      source_effective_balance
    state := { state with balances }
    next_pending_consolidation := next_pending_consolidation + 1

  pure { state with
    pending_consolidations := state.pending_consolidations.drop next_pending_consolidation }

end EpochProofs.Spec
