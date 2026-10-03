import EpochProofs.Spec.BuilderPendingPayments

/-!
# Sanity theorems for the `process_builder_pending_payments` reference

Facts about the reference alone. If a transcription error breaks a fact, the build fails.

The Lighthouse proof targets `process_builder_pending_payments_eq`. It has no loop and no
slice assignment.
-/

namespace EpochProofs.Spec

/-- The loop appends the withdrawal of each payment at or above the quorum, in order.
It never fails. -/
theorem withdrawals_loop (quorum : Uint64) (payments : List BuilderPendingPayment)
    (acc : List BuilderPendingWithdrawal) :
    (forIn payments acc fun payment r =>
        if payment.weight ≥ quorum then Except.ok (ForInStep.yield (r ++ [payment.withdrawal]))
        else Except.ok (ForInStep.yield r) : SpecM _) =
      Except.ok (acc ++ (payments.filter (·.weight ≥ quorum)).map (·.withdrawal)) := by
  induction payments generalizing acc with
  | nil => simp [forIn, ForIn.forIn, pure, Except.pure]
  | cons payment payments ih =>
    rw [List.forIn_cons]
    by_cases h : payment.weight ≥ quorum <;>
      simp [h, ih, bind, Except.bind]

/-- The state after `process_builder_pending_payments` for a given quorum. -/
def builderPendingPaymentsResult (p : Preset) (quorum : Uint64) (state : BeaconState) :
    BeaconState :=
  { builder_pending_payments :=
      state.builder_pending_payments.drop p.SLOTS_PER_EPOCH
        ++ List.replicate p.SLOTS_PER_EPOCH BuilderPendingPayment.empty
    builder_pending_withdrawals :=
      state.builder_pending_withdrawals
        ++ ((state.builder_pending_payments.take p.SLOTS_PER_EPOCH).filter
              (·.weight ≥ quorum)).map (·.withdrawal) }

theorem process_builder_pending_payments_eq (p : Preset) (total_active_balance : Gwei)
    (state : BeaconState) (h : state.WellFormed p) :
    process_builder_pending_payments p total_active_balance state =
      (get_builder_payment_quorum_threshold p total_active_balance).map
        (builderPendingPaymentsResult p · state) := by
  unfold process_builder_pending_payments
  simp only [bind, Except.bind, pure, Except.pure]
  cases get_builder_payment_quorum_threshold p total_active_balance with
  | error e => rfl
  | ok quorum =>
    simp only [withdrawals_loop, Except.map, builderPendingPaymentsResult]
    have hlen : (state.builder_pending_payments.drop p.SLOTS_PER_EPOCH).length
        = p.SLOTS_PER_EPOCH := by
      simp [List.length_drop, BeaconState.WellFormed] at h ⊢; omega
    rw [List.take_left' hlen]

/-- The payments vector keeps length `2 * SLOTS_PER_EPOCH`. -/
theorem process_builder_pending_payments_wellFormed (p : Preset) (total_active_balance : Gwei)
    (state state' : BeaconState) (h : state.WellFormed p)
    (hok : process_builder_pending_payments p total_active_balance state = .ok state') :
    state'.WellFormed p := by
  rw [process_builder_pending_payments_eq p total_active_balance state h] at hok
  cases hq : get_builder_payment_quorum_threshold p total_active_balance with
  | error e => simp [hq, Except.map] at hok
  | ok quorum =>
    simp only [hq, Except.map, Except.ok.injEq] at hok
    subst hok
    simp [builderPendingPaymentsResult, BeaconState.WellFormed] at h ⊢
    omega

/-- If `SLOTS_PER_EPOCH ≥ 6`, the quorum does not overflow. Both presets satisfy this. -/
theorem get_builder_payment_quorum_threshold_ok (p : Preset) (total_active_balance : Gwei)
    (hslots : 6 ≤ p.SLOTS_PER_EPOCH) (hbalance : total_active_balance < UINT64_SIZE) :
    get_builder_payment_quorum_threshold p total_active_balance =
      .ok (total_active_balance / p.SLOTS_PER_EPOCH * 6 / 10) := by
  have hmul : total_active_balance / p.SLOTS_PER_EPOCH * 6 < UINT64_SIZE :=
    calc total_active_balance / p.SLOTS_PER_EPOCH * 6
        ≤ total_active_balance / p.SLOTS_PER_EPOCH * p.SLOTS_PER_EPOCH :=
          Nat.mul_le_mul_left _ hslots
      _ ≤ total_active_balance := Nat.div_mul_le_self _ _
      _ < UINT64_SIZE := hbalance
  have hslots0 : p.SLOTS_PER_EPOCH ≠ 0 := by omega
  simp [get_builder_payment_quorum_threshold, uint64Div, uint64Mul, hslots0, hmul,
    BUILDER_PAYMENT_THRESHOLD_NUMERATOR, BUILDER_PAYMENT_THRESHOLD_DENOMINATOR, bind,
    Except.bind, pure, Except.pure]

end EpochProofs.Spec
