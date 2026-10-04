import EpochProofs.Sanity.Invariants.LighthouseEpoch
import EpochProofs.Sanity.Invariants.PubkeysUnique
import EpochProofs.Sanity.LighthouseRow

/-!
# The Lighthouse epoch tail: the model

Lighthouse runs the pending deposits and the effective balance updates inside its single pass.
Before the pass it decides the deposits once, on statuses that it predicts from the state before
the registry update. Inside the pass each row gets the sum of its top-ups and, unless a pending
consolidation names it, its effective balance update. After the pass it writes the deposit
queue, adds the new validators, updates their effective balances, runs the consolidations,
redoes the effective balance update for each validator that a consolidation names, and runs the
builder payments. `process_epoch_lh_full` models these steps on mainnet.
-/

namespace EpochProofs.Spec

/-- The flags that Lighthouse predicts for the validator of a deposit, from the state before the
single pass. A deposit for an unknown pubkey has neither flag. -/
def lhDepositView (s : BeaconState) (current_epoch : Epoch) (d : PendingDeposit) :
    DepositSpecView :=
  match s.validators[(s.validators.map (·.pubkey)).idxOf d.pubkey]? with
  | some v =>
    let st := predictedStatus Preset.mainnet true true (registryFieldsOf v (0, 0))
      v.effective_balance current_epoch (current_epoch + 1)
    ⟨d.slot, d.amount, false, st.1, st.2⟩
  | none => ⟨d.slot, d.amount, false, false, false⟩

/-- What Lighthouse decides about the pending deposits before the single pass. -/
structure LhDepositPlan where
  next_deposit_index : Nat
  deposit_balance_to_consume : Gwei
  /-- The sum of the top-ups for each existing validator, by index. -/
  topups : List Gwei
  postponed : List PendingDeposit
  /-- The deposits for pubkeys that the state does not know yet, in queue order. -/
  new_validator_deposits : List PendingDeposit

/-- One handled deposit: postponed, a top-up of a known validator, or a new validator deposit.
The top-up sums use checked additions, as `safe_add_assign` does. -/
def lhPlanStep (pubkeys : List BLSPubkey) (acc : List Gwei × List PendingDeposit ×
    List PendingDeposit) (x : PendingDeposit × Bool) :
    SpecM (List Gwei × List PendingDeposit × List PendingDeposit) :=
  let (sums, post, news) := acc
  let (d, postpone) := x
  if postpone then pure (sums, post ++ [d], news)
  else if d.pubkey ∈ pubkeys then do
    let sums ← increase_balance sums (pubkeys.idxOf d.pubkey) d.amount
    pure (sums, post, news)
  else pure (sums, post, news ++ [d])

/-- `PendingDepositsContext::new`: the decisions on the first `MAX_PENDING_DEPOSITS_PER_EPOCH`
deposits, then the top-up sums, the postponed deposits and the new validator deposits. -/
def lhDepositPlan (s : BeaconState) (total_active_balance : Gwei) (current_epoch : Epoch) :
    SpecM LhDepositPlan := do
  let finalized_slot ←
    compute_start_slot_at_epoch Preset.mainnet s.finalized_checkpoint.epoch
  let views := (s.pending_deposits.take Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH).map
    (lhDepositView s current_epoch)
  let (next, dbtc, postponed) ← depositDecisions Preset.mainnet finalized_slot
    s.deposit_balance_to_consume (← get_activation_churn_limit Preset.mainnet total_active_balance)
    views
  let (topups, post, news) ← ((s.pending_deposits.take next).zip postponed).foldlM
    (lhPlanStep (s.validators.map (·.pubkey))) (List.replicate s.validators.length 0, [], [])
  pure ⟨next, dbtc, topups, post, news⟩

/-- The effective balance update of one validator. -/
def ebUpdateAt (s : BeaconState) (i : Nat) : SpecM BeaconState := do
  let v ← listGet s.validators i
  let b ← listGet s.balances i
  let (downward, upward) ← hysteresisThresholds Preset.mainnet
  let effective_balance ← newEffectiveBalance Preset.mainnet downward upward v b
  pure { s with validators := ← listSet s.validators i { v with effective_balance } }

/-- The validators that the pending consolidations name, as sources or targets, sorted and
without duplicates. -/
def consolidationIndices (s : BeaconState) : List ValidatorIndex :=
  ((s.pending_consolidations.flatMap fun c => [c.source_index, c.target_index]).eraseDups).mergeSort

/-- The steps after the builder payments: they are the same in Lighthouse and the spec. -/
def epochRest2 (o : Oracle) (state : BeaconState) : SpecM BeaconState := do
  let state ← process_slashings_reset Preset.mainnet state
  let state ← process_randao_mixes_reset Preset.mainnet state
  let state ← process_historical_summaries_update Preset.mainnet o state
  let state := process_participation_flag_updates state
  let state ← process_sync_committee_updates Preset.mainnet o state
  let state ← process_proposer_lookahead Preset.mainnet o state
  process_ptc_window Preset.mainnet o state

/-- The Lighthouse steps after justification and finalization, away from the genesis epoch. -/
def lhFullTail (o : Oracle) (s : BeaconState) : SpecM BeaconState := do
  let current_epoch ← get_current_epoch Preset.mainnet s
  let total_active_balance ← get_total_active_balance Preset.mainnet s
  let rctx ← rewardsContextOf Preset.mainnet total_active_balance s
  let (_, per) ← slashingsPreamble Preset.mainnet total_active_balance s.slashings
  let plan ← lhDepositPlan s total_active_balance current_epoch
  let (downward, upward) ← hysteresisThresholds Preset.mainnet
  let named := consolidationIndices s
  let ctx := lhContextOf s total_active_balance current_epoch rctx per
  let (churn, rows) ← passM (fun churn r => lhRowStepFull Preset.mainnet ctx downward upward
      ⟨lhBaseReward total_active_balance r, plan.topups.getD r.index 0, decide (r.index ∈ named)⟩
      churn r)
    (s.earliest_exit_epoch, s.exit_balance_to_consume) (rowsOf s)
  let s1 ← process_eth1_data_reset Preset.mainnet (s.withRowsChurn churn rows)
  let s2 := { s1 with
    pending_deposits := s.pending_deposits.drop plan.next_deposit_index ++ plan.postponed
    deposit_balance_to_consume := plan.deposit_balance_to_consume }
  let s3 ← plan.new_validator_deposits.foldlM (apply_pending_deposit Preset.mainnet o) s2
  let s4 ← (List.range' s2.validators.length (s3.validators.length - s2.validators.length)).foldlM
    ebUpdateAt s3
  let s5 ← process_pending_consolidations Preset.mainnet s4
  let s6 ← named.foldlM ebUpdateAt s5
  let s7 ← process_builder_pending_payments Preset.mainnet total_active_balance s6
  epochRest2 o s7

/-- The Gloas `process_epoch` on mainnet as Lighthouse runs it, with the pending deposits and the
effective balance updates in the single pass. At the genesis epoch the spec skips the inactivity
and rewards steps; Lighthouse skips them too, and this one epoch uses the spec path. -/
def process_epoch_lh_full (o : Oracle) (s : BeaconState) : SpecM BeaconState :=
  if s.slot / Preset.mainnet.SLOTS_PER_EPOCH = GENESIS_EPOCH then process_epoch Preset.mainnet o s
  else do
    let s1 ← process_justification_and_finalization Preset.mainnet s
    lhFullTail o s1

/-! ## Helpers for the proofs -/

/-- `s` with effective balance `e` for validator `i`. An index out of range changes nothing. -/
def setEB (s : BeaconState) (i : Nat) (e : Gwei) : BeaconState :=
  match s.validators[i]? with
  | some v => { s with validators := s.validators.set i { v with effective_balance := e } }
  | none => s

/-- The spec's flags for the validator of a deposit, read in state `s`. -/
def specDepositView (s : BeaconState) (next_epoch : Epoch) (d : PendingDeposit) :
    DepositSpecView :=
  match s.validators[(s.validators.map (·.pubkey)).idxOf d.pubkey]? with
  | some v => ⟨d.slot, d.amount, false, decide (v.exit_epoch < FAR_FUTURE_EPOCH),
      decide (v.withdrawable_epoch < next_epoch)⟩
  | none => ⟨d.slot, d.amount, false, false, false⟩

/-- Apply the handled deposits that are not postponed, in queue order. -/
def applyHandled (o : Oracle) (s : BeaconState) (handled : List (PendingDeposit × Bool)) :
    SpecM BeaconState :=
  handled.foldlM (fun s x => if x.2 then pure s else apply_pending_deposit Preset.mainnet o s x.1) s

end EpochProofs.Spec
