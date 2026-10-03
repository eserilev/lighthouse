import EpochProofs.Equiv.RegistryUpdates
import EpochProofs.Sanity.PendingDeposits

/-!
# Lighthouse pending deposits decisions equal the reference

`process_pending_deposits_equiv` relates `process_pending_deposits` in `pending_deposits.rs` to
`depositDecisions`, over the views that `single_pass.rs` builds. `deposit_status_eq` shows that
the flags of a view are `predictedStatus`. `predictedStatus_eq_post_registry` then relates them
to the spec's flags after the registry update.

Lighthouse builds the views from the pubkey cache and applies the decisions in `single_pass.rs`.
The theorems take the views as inputs.
-/

namespace EpochProofs

open Aeneas Aeneas.Std Aeneas.Std.WP Result state_processing

abbrev LhDepositView := per_epoch_processing.pending_deposits.DepositView
abbrev LhDepositConstants := per_epoch_processing.pending_deposits.DepositConstants

structure DepositConstantsMatch (p : Spec.Preset) (c : LhDepositConstants) : Prop where
  far_future_epoch : c.far_future_epoch.val = Spec.FAR_FUTURE_EPOCH
  ejection_balance : c.ejection_balance.val = p.EJECTION_BALANCE
  max_pending_deposits_per_epoch :
    c.max_pending_deposits_per_epoch.val = p.MAX_PENDING_DEPOSITS_PER_EPOCH

/-- The validator epochs of a view. -/
def viewFields (v : LhDepositView) : Spec.RegistryFields :=
  ⟨0, v.activation_epoch.val, v.exit_epoch.val, v.withdrawable_epoch.val, 0, 0⟩

/-- The spec view of a Lighthouse view. -/
def specView (p : Spec.Preset) (c : LhDepositConstants) (current_epoch next_epoch : Nat)
    (v : LhDepositView) : Spec.DepositSpecView :=
  let status := Spec.predictedStatus p c.registry_updates v.is_known_validator (viewFields v)
    v.effective_balance.val current_epoch next_epoch
  ⟨v.slot.val, v.amount.val, v.eth1_bridge_blocked, status.1, status.2⟩

theorem deposit_status_eq (p : Spec.Preset) (v : LhDepositView) (current_epoch next_epoch : U64)
    (c : LhDepositConstants) (hc : DepositConstantsMatch p c) :
    per_epoch_processing.pending_deposits.deposit_status v current_epoch next_epoch c =
      ok (Spec.predictedStatus p c.registry_updates v.is_known_validator (viewFields v)
        v.effective_balance.val current_epoch.val next_epoch.val) := by
  unfold per_epoch_processing.pending_deposits.deposit_status Spec.predictedStatus viewFields
  cases hk : v.is_known_validator
  · simp
  · simp only [if_true, Bool.not_true, Bool.false_eq_true, if_false, UScalar.le_equiv,
      UScalar.lt_equiv, hc.far_future_epoch, hc.ejection_balance]
    by_cases ha : v.activation_epoch.val ≤ current_epoch.val <;>
      by_cases hb : current_epoch.val < v.exit_epoch.val <;>
      by_cases he : v.exit_epoch.val < Spec.FAR_FUTURE_EPOCH <;>
      cases hr : c.registry_updates <;>
      simp [ha, hb, he]

theorem slice_get_any {T : Type} (s : Slice T) (i : Usize) :
    core.slice.Slice.get (core.slice.index.SliceIndexUsizeSlice T) s i = ok s.val[i.val]? :=
  rfl

theorem u64_saturating_add_one (x : U64) (h : x.val < U64.max) :
    (core.num.U64.saturating_add x 1#u64).val = x.val + 1 := by
  have hmax : U64.max < 2 ^ UScalarTy.U64.numBits := by
    rw [U64.max_def]; simp [U64.numBits]
  simp only [core.num.U64.saturating_add, UScalar.saturating_add, UScalar.val,
    BitVec.toNat_ofNat, UScalar.max_UScalarTy_U64_eq]
  rw [Nat.mod_eq_of_lt (by omega)]
  simp at h ⊢; omega

/-- The spec views of a slice of Lighthouse views. -/
abbrev specViews (p : Spec.Preset) (c : LhDepositConstants) (current_epoch next_epoch : U64)
    (views : Slice LhDepositView) : List Spec.DepositSpecView :=
  views.val.map (specView p c current_epoch.val next_epoch.val)

theorem deposits_loop_spec (p : Spec.Preset) (views : Slice LhDepositView)
    (finalized_slot current_epoch next_epoch available : U64) (c : LhDepositConstants)
    (hc : DepositConstantsMatch p c)
    (processed index : U64) (postponed : alloc.vec.Vec Bool) (i : Usize)
    (hi : i.val ≤ views.val.length) (hidx : index.val ≤ i.val)
    (hpp : postponed.val.length ≤ i.val)
    (hst : Spec.depositLoop p finalized_slot.val available.val ⟨0, 0, []⟩
      ((specViews p c current_epoch next_epoch views).take i.val) =
      .ok ⟨⟨processed.val, index.val, postponed.val⟩, false, false⟩) :
    per_epoch_processing.pending_deposits.process_pending_deposits_loop views finalized_slot
      current_epoch next_epoch c available processed index postponed i ⦃ r =>
        (r.2.2.2.1 = true → Spec.depositLoop p finalized_slot.val available.val ⟨0, 0, []⟩
            (specViews p c current_epoch next_epoch views) = .error .overflow)
        ∧ (r.2.2.2.1 = false → ∃ stopped, Spec.depositLoop p finalized_slot.val available.val
            ⟨0, 0, []⟩ (specViews p c current_epoch next_epoch views) =
              .ok ⟨⟨r.1.val, r.2.1.val, r.2.2.2.2.val⟩, stopped, r.2.2.1⟩) ⦄ := by
  unfold per_epoch_processing.pending_deposits.process_pending_deposits_loop
  apply loop.spec_decr_nat
    (measure := fun (x : U64 × U64 × alloc.vec.Vec Bool × Usize) =>
      views.val.length - x.2.2.2.val)
    (inv := fun (x : U64 × U64 × alloc.vec.Vec Bool × Usize) =>
      x.2.2.2.val ≤ views.val.length ∧ x.2.1.val ≤ x.2.2.2.val
      ∧ x.2.2.1.val.length ≤ x.2.2.2.val
      ∧ Spec.depositLoop p finalized_slot.val available.val ⟨0, 0, []⟩
          ((specViews p c current_epoch next_epoch views).take x.2.2.2.val) =
          .ok ⟨⟨x.1.val, x.2.1.val, x.2.2.1.val⟩, false, false⟩)
  · rintro ⟨pa, idx, pp, j⟩ ⟨hj, hidxj, hppj, hstj⟩
    dsimp only at hj hidxj hppj hstj
    unfold per_epoch_processing.pending_deposits.process_pending_deposits_loop.body
    simp only [slice_get_any, lift, bind_tc_ok]
    split
    · rename_i hlt
      have hjl : j.val < views.val.length := by scalar_tac
      have hlen : views.val.length ≤ Usize.max := by scalar_tac
      simp only [List.getElem?_eq_getElem hjl]
      have hget : (specViews p c current_epoch next_epoch views)[j.val]? =
          some (specView p c current_epoch.val next_epoch.val views.val[j.val]) := by
        simp [specViews, hjl]
      by_cases h1 : views.val[j.val].eth1_bridge_blocked = true
      · simp only [h1, if_true, spec_ok]
        refine ⟨by simp, fun _ => ⟨true, ?_⟩⟩
        exact Spec.depositLoop_stop_at p _ _ _ _ _ j.val _ hget hstj _
          (by simp [Spec.depositStep, specView, h1, pure, Except.pure]) rfl
      simp only [h1, Bool.false_eq_true, if_false]
      by_cases h2 : views.val[j.val].slot.val > finalized_slot.val
      · have h2' : views.val[j.val].slot > finalized_slot := by simp [UScalar.lt_equiv]; exact h2
        simp only [h2', if_true, spec_ok]
        refine ⟨by simp, fun _ => ⟨true, ?_⟩⟩
        exact Spec.depositLoop_stop_at p _ _ _ _ _ j.val _ hget hstj _
          (by simp [Spec.depositStep, specView, h1, h2, pure, Except.pure]) rfl
      have h2' : ¬ views.val[j.val].slot > finalized_slot := by simp [UScalar.lt_equiv]; omega
      simp only [h2', if_false]
      by_cases h3 : idx.val ≥ p.MAX_PENDING_DEPOSITS_PER_EPOCH
      · have h3' : idx ≥ c.max_pending_deposits_per_epoch := by
          simp [UScalar.le_equiv, hc.max_pending_deposits_per_epoch]; exact h3
        simp only [h3', if_true, spec_ok]
        refine ⟨by simp, fun _ => ⟨true, ?_⟩⟩
        exact Spec.depositLoop_stop_at p _ _ _ _ _ j.val _ hget hstj _
          (by simp [Spec.depositStep, specView, h1, h2, h3, pure, Except.pure]) rfl
      have h3' : ¬ idx ≥ c.max_pending_deposits_per_epoch := by
        simp [UScalar.le_equiv, hc.max_pending_deposits_per_epoch]; omega
      simp only [h3', if_false]
      rw [deposit_status_eq p _ _ _ c hc, bind_tc_ok]
      generalize hps : Spec.predictedStatus p c.registry_updates
        views.val[j.val].is_known_validator (viewFields views.val[j.val])
        views.val[j.val].effective_balance.val current_epoch.val next_epoch.val = ps
      obtain ⟨exited, withdrawn⟩ := ps
      simp only [uncurry_apply_pair]
      have hsv : specView p c current_epoch.val next_epoch.val views.val[j.val] =
          ⟨views.val[j.val].slot.val, views.val[j.val].amount.val,
            views.val[j.val].eth1_bridge_blocked, exited, withdrawn⟩ := by
        simp only [specView, hps]
      rw [hsv] at hget
      have hjmax : j.val < Usize.max := by omega
      have hsat := usize_saturating_add_one j hjmax
      have hus : Usize.max ≤ U64.max := by
        rcases Usize.bounds_eq with h | h <;> rw [h] <;> simp [U32.max_eq, U64.max_eq]
      have hidxmax : idx.val < U64.max := by omega
      have hsat2 := u64_saturating_add_one idx hidxmax
      have hpush : pp.val.length < Usize.max := by omega
      have hn2 : ¬ views.val[j.val].slot.val > finalized_slot.val := h2
      have hn3 : ¬ idx.val ≥ p.MAX_PENDING_DEPOSITS_PER_EPOCH := h3
      cases withdrawn
      · cases exited
        · -- The churn path.
          simp only [Bool.false_eq_true, if_false]
          have hca := U64.checked_add_bv_spec pa views.val[j.val].amount
          simp only [U64.max_eq] at hca
          cases hcadd : pa.checked_add views.val[j.val].amount with
          | none =>
            simp only [hcadd] at hca
            simp only [spec_ok]
            refine ⟨fun _ => ?_, by simp⟩
            have hov : ¬ pa.val + views.val[j.val].amount.val < 18446744073709551616 := by omega
            exact Spec.depositLoop_error_at p _ _ _ _ _ j.val _ hget hstj _
              (by simp [Spec.depositStep, h1, hn2, hn3, Spec.uint64Add, Spec.UINT64_SIZE, hov,
                Bind.bind, Except.bind, throw, throwThe,
                MonadExceptOf.throw])
          | some total =>
            simp only [hcadd] at hca
            have hfit : pa.val + views.val[j.val].amount.val < 18446744073709551616 := by omega
            by_cases hgt : total.val > available.val
            · have hgt' : total > available := by simp [UScalar.lt_equiv]; exact hgt
              simp only [hgt', if_true, spec_ok]
              refine ⟨by simp, fun _ => ⟨true, ?_⟩⟩
              rw [hca.2.1] at hgt
              exact Spec.depositLoop_stop_at p _ _ _ _ _ j.val _ hget hstj _
                (by simp [Spec.depositStep, h1, hn2, hn3, Spec.uint64Add, Spec.UINT64_SIZE,
                  hfit, hgt, Bind.bind, Except.bind, pure, Except.pure]) rfl
            · have hgt' : ¬ total > available := by simp [UScalar.lt_equiv]; omega
              simp only [hgt', if_false]
              step as ⟨pp1, hpp1⟩
              refine ⟨by omega, by omega, by simp [hpp1]; omega, ?_, by omega⟩
              rw [hca.2.1] at hgt
              simp only [hsat, hsat2, hpp1, hca.2.1]
              exact Spec.depositLoop_next_at p _ _ _ _ _ j.val _ hget hstj _
                (by simp [Spec.depositStep, h1, hn2, hn3, Spec.uint64Add, Spec.UINT64_SIZE,
                  hfit, hgt, Bind.bind, Except.bind, pure, Except.pure])
        · -- Exited: postpone.
          simp only [Bool.false_eq_true, if_false, if_true]
          step as ⟨pp1, hpp1⟩
          refine ⟨by omega, by omega, by simp [hpp1]; omega, ?_, by omega⟩
          simp only [hsat, hsat2, hpp1]
          exact Spec.depositLoop_next_at p _ _ _ _ _ j.val _ hget hstj _
            (by simp [Spec.depositStep, h1, hn2, hn3, pure, Except.pure])
      · -- Withdrawn: apply without churn.
        simp only [if_true]
        step as ⟨pp1, hpp1⟩
        refine ⟨by omega, by omega, by simp [hpp1]; omega, ?_, by omega⟩
        simp only [hsat, hsat2, hpp1]
        exact Spec.depositLoop_next_at p _ _ _ _ _ j.val _ hget hstj _
          (by simp [Spec.depositStep, h1, hn2, hn3, pure, Except.pure])
    · rename_i hge
      have hjl : views.val.length ≤ j.val := by scalar_tac
      simp only [spec_ok]
      refine ⟨by simp, fun _ => ⟨false, ?_⟩⟩
      exact Spec.depositLoop_end p _ _ _ _ j.val (by simp [specViews]; omega) _ hstj
  · exact ⟨hi, hidx, hpp, hst⟩

def absDeposits :
    core.result.Result per_epoch_processing.pending_deposits.DepositsOutcome
      safe_arith.ArithError → Spec.SpecM (Nat × Spec.Gwei × List Bool)
  | .Ok o => .ok (o.next_deposit_index.val, o.deposit_balance_to_consume.val, o.postponed.val)
  | .Err e => .error (absArithError e)

theorem process_pending_deposits_equiv (p : Spec.Preset) (views : Slice LhDepositView)
    (finalized_slot current_epoch next_epoch deposit_balance_to_consume churn : U64)
    (c : LhDepositConstants) (hc : DepositConstantsMatch p c) :
    per_epoch_processing.pending_deposits.process_pending_deposits views finalized_slot
      current_epoch next_epoch deposit_balance_to_consume churn c ⦃ r =>
        absDeposits r = Spec.depositDecisions p finalized_slot.val deposit_balance_to_consume.val
          churn.val (specViews p c current_epoch next_epoch views) ⦄ := by
  unfold per_epoch_processing.pending_deposits.process_pending_deposits
  apply exists_imp_spec
  obtain ⟨r1, hr1, h1⟩ := spec_imp_exists (safe_add_spec deposit_balance_to_consume churn)
  rw [hr1]
  cases r1 with
  | Err e =>
    obtain ⟨rfl, hov⟩ := h1.2 e rfl
    refine ⟨_, rfl, ?_⟩
    simp [Spec.depositDecisions, hov, absDeposits, absArithError, except_ok_bind, except_error_bind, Pure.pure, Except.pure,
      throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.uint64Sub, Spec.UINT64_SIZE,
      
      
      core.convert.FromSame.from]
  | Ok avail =>
  obtain ⟨hav, hfit⟩ := h1.1 avail rfl
  simp only [core.result.Result.Insts.CoreOpsTry.branch, bind_tc_ok]
  have hinit : Spec.depositLoop p finalized_slot.val avail.val ⟨0, 0, []⟩
      ((specViews p c current_epoch next_epoch views).take (0#usize : Usize).val) =
      .ok ⟨⟨(0#u64 : U64).val, (0#u64 : U64).val,
        (alloc.vec.Vec.new Bool : alloc.vec.Vec Bool).val⟩, false, false⟩ := by
    simp [Spec.depositLoop, Pure.pure, Except.pure]
  obtain ⟨res, hres, hpost⟩ := spec_imp_exists (deposits_loop_spec p views finalized_slot
    current_epoch next_epoch avail c hc 0#u64 0#u64 (alloc.vec.Vec.new Bool) 0#usize
    (by simp) (by simp) (by simp) hinit)
  rw [hres]
  obtain ⟨pa, idx, reached, ovf, pp⟩ := res
  simp only [uncurry_apply_pair, bind_tc_ok]
  cases ovf
  · obtain ⟨stopped, hloop⟩ := hpost.2 rfl
    rw [hav] at hloop
    simp only [Bool.false_eq_true, if_false]
    cases reached
    · refine ⟨_, rfl, ?_⟩
      simp [Spec.depositDecisions, hfit, hloop, absDeposits, except_ok_bind, Pure.pure, Except.pure,
      throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.uint64Sub, Spec.UINT64_SIZE,
      
      
      ]
    · simp only [if_true]
      obtain ⟨r2, hr2, h2⟩ := spec_imp_exists (safe_sub_spec avail pa)
      rw [hr2]
      cases r2 with
      | Err e =>
        obtain ⟨rfl, hlt⟩ := h2.2 e rfl
        refine ⟨_, rfl, ?_⟩
        rw [hav] at hlt
        have hn : ¬ pa.val ≤ deposit_balance_to_consume.val + churn.val := by omega
        simp [Spec.depositDecisions, hfit, hloop, hn, absDeposits, absArithError, except_ok_bind, except_error_bind, Pure.pure, Except.pure,
      throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.uint64Sub, Spec.UINT64_SIZE,
      
      
      core.convert.FromSame.from]
      | Ok d =>
        obtain ⟨hd, hle⟩ := h2.1 d rfl
        refine ⟨_, rfl, ?_⟩
        rw [hav] at hd hle
        simp [Spec.depositDecisions, hfit, hloop, hd, hle, absDeposits, except_ok_bind, Pure.pure, Except.pure,
      throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.uint64Sub, Spec.UINT64_SIZE,
      
      
      ]
  · have hloop := hpost.1 rfl
    rw [hav] at hloop
    simp only [if_true]
    refine ⟨_, rfl, ?_⟩
    simp [Spec.depositDecisions, hfit, hloop, absDeposits, absArithError, except_ok_bind, except_error_bind, Pure.pure, Except.pure,
      throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.uint64Sub, Spec.UINT64_SIZE,
      
      
      ]

end EpochProofs
