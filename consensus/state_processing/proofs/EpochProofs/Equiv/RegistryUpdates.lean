import EpochProofs.Equiv.RewardsAndPenalties
import EpochProofs.Sanity.RegistryUpdates

/-!
# Lighthouse registry update equals the reference

`registry_update_equiv` relates `registry_update` in `registry_update.rs` to
`registryStepIndependent`. `registry_update_eq_spec` then uses
`registryStepIndependent_eq_exclusive` to relate it to the spec's loop body on valid states.

The theorems cover Gloas: the exit churn has no upper limit, so `exit_churn_cap` is
`u64::MAX`. Lighthouse reads the validator fields, the churn state and the finalized epoch in
`single_pass.rs`. The theorems take them as inputs.
-/

namespace EpochProofs

open Aeneas Aeneas.Std Aeneas.Std.WP Result state_processing

abbrev LhRegistryFields := per_epoch_processing.registry_update.RegistryFields
abbrev LhRegistryConstants := per_epoch_processing.registry_update.RegistryConstants

def absRegistry (f : LhRegistryFields) : Spec.RegistryFields :=
  { activation_eligibility_epoch := f.activation_eligibility_epoch.val
    activation_epoch := f.activation_epoch.val
    exit_epoch := f.exit_epoch.val
    withdrawable_epoch := f.withdrawable_epoch.val
    earliest_exit_epoch := f.earliest_exit_epoch.val
    exit_balance_to_consume := f.exit_balance_to_consume.val }

/-- The Lighthouse constants are the Gloas spec values for preset `p`. -/
structure ConstantsMatch (p : Spec.Preset) (c : LhRegistryConstants) : Prop where
  far_future_epoch : c.far_future_epoch.val = Spec.FAR_FUTURE_EPOCH
  min_activation_balance : c.min_activation_balance.val = p.MIN_ACTIVATION_BALANCE
  ejection_balance : c.ejection_balance.val = p.EJECTION_BALANCE
  max_seed_lookahead : c.max_seed_lookahead.val = p.MAX_SEED_LOOKAHEAD
  min_validator_withdrawability_delay :
    c.min_validator_withdrawability_delay.val = p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY
  min_per_epoch_churn_limit : c.min_per_epoch_churn_limit.val = p.MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA
  churn_limit_quotient : c.churn_limit_quotient.val = p.CHURN_LIMIT_QUOTIENT_GLOAS
  effective_balance_increment : c.effective_balance_increment.val = p.EFFECTIVE_BALANCE_INCREMENT
  exit_churn_cap : c.exit_churn_cap.val = 18446744073709551615

theorem safe_sub_spec (a b : U64) :
    U64.Insts.Safe_arithSafeArithU64.safe_sub a b ⦃ r =>
      (∀ v, r = .Ok v → v.val = a.val - b.val ∧ b.val ≤ a.val)
      ∧ (∀ e, r = .Err e → e = .Overflow ∧ a.val < b.val) ⦄ := by
  unfold U64.Insts.Safe_arithSafeArithU64.safe_sub
  have h := U64.checked_sub_bv_spec a b
  cases hc : a.checked_sub b <;> simp only [hc] at h
  · simp [lift]; omega
  · simp [lift, h.2.1]; omega

theorem safe_mul_spec (a b : U64) :
    U64.Insts.Safe_arithSafeArithU64.safe_mul a b ⦃ r =>
      (∀ v, r = .Ok v → v.val = a.val * b.val ∧ a.val * b.val < 18446744073709551616)
      ∧ (∀ e, r = .Err e → e = .Overflow ∧ ¬ a.val * b.val < 18446744073709551616) ⦄ := by
  unfold U64.Insts.Safe_arithSafeArithU64.safe_mul
  have h := U64.checked_mul_bv_spec a b
  simp only [U64.max_eq] at h
  cases hc : a.checked_mul b <;> simp only [hc] at h
  · simp [lift]; omega
  · simp [lift, h.2.1]; omega

theorem safe_div_spec (a b : U64) :
    U64.Insts.Safe_arithSafeArithU64.safe_div a b ⦃ r =>
      (∀ v, r = .Ok v → v.val = a.val / b.val ∧ b.val ≠ 0)
      ∧ (∀ e, r = .Err e → e = .DivisionByZero ∧ b.val = 0) ⦄ := by
  unfold U64.Insts.Safe_arithSafeArithU64.safe_div
  have h := U64.checked_div_bv_spec a b
  cases hc : a.checked_div b <;> simp only [hc] at h
  · simp [lift, h]
  · simp [lift, h.2.1, h.1]

theorem safe_rem_spec (a b : U64) :
    U64.Insts.Safe_arithSafeArithU64.safe_rem a b ⦃ r =>
      (∀ v, r = .Ok v → v.val = a.val % b.val ∧ b.val ≠ 0)
      ∧ (∀ e, r = .Err e → e = .DivisionByZero ∧ b.val = 0) ⦄ := by
  unfold U64.Insts.Safe_arithSafeArithU64.safe_rem
  have h := U64.checked_rem_bv_spec a b
  cases hc : a.checked_rem b <;> simp only [hc] at h
  · simp [lift, h]
  · simp [lift, h.2.1, h.1]

theorem compute_activation_exit_epoch_equiv (p : Spec.Preset) (epoch lookahead : U64)
    (hlookahead : lookahead.val = p.MAX_SEED_LOOKAHEAD) :
    per_epoch_processing.registry_update.compute_activation_exit_epoch epoch lookahead ⦃ r =>
      absResult (·.val) r = Spec.compute_activation_exit_epoch p epoch.val ⦄ := by
  unfold per_epoch_processing.registry_update.compute_activation_exit_epoch
  apply exists_imp_spec
  obtain ⟨r1, hr1, h1⟩ := spec_imp_exists (safe_add_spec epoch 1#u64)
  rw [hr1]
  have hone : (1#u64 : U64).val = 1 := rfl
  cases r1 with
  | Err e =>
    obtain ⟨rfl, hov⟩ := h1.2 e rfl
    simp only [hone] at hov
    refine ⟨_, rfl, ?_⟩
    simp [Spec.compute_activation_exit_epoch, Spec.uint64Add, Spec.UINT64_SIZE, hov,
      absResult, absArithError, 
      
      except_error_bind, throw, throwThe, MonadExceptOf.throw]
  | Ok v =>
  obtain ⟨hv, hfit⟩ := h1.1 v rfl
  simp only [hone] at hv hfit
  simp only [core.result.Result.Insts.CoreOpsTry.branch, bind_tc_ok]
  obtain ⟨r2, hr2, h2⟩ := spec_imp_exists (safe_add_spec v lookahead)
  rw [hr2]
  cases r2 with
  | Err e =>
    obtain ⟨rfl, hov⟩ := h2.2 e rfl
    simp only [hv, hlookahead] at hov
    refine ⟨_, rfl, ?_⟩
    simp [Spec.compute_activation_exit_epoch, Spec.uint64Add, Spec.UINT64_SIZE, hfit, hov,
      absResult, absArithError, throw, throwThe, MonadExceptOf.throw]
  | Ok w =>
  obtain ⟨hw, hfit2⟩ := h2.1 w rfl
  simp only [hv, hlookahead] at hw hfit2
  refine ⟨_, rfl, ?_⟩
  simp [Spec.compute_activation_exit_epoch, Spec.uint64Add, Spec.UINT64_SIZE, hfit, hfit2, hw,
    absResult, except_ok_bind, Pure.pure, Except.pure]

theorem exit_churn_limit_equiv (p : Spec.Preset) (total_active_balance : U64)
    (c : LhRegistryConstants) (hc : ConstantsMatch p c) :
    per_epoch_processing.registry_update.exit_churn_limit total_active_balance c ⦃ r =>
      absResult (·.val) r = Spec.get_exit_churn_limit p total_active_balance.val ⦄ := by
  unfold per_epoch_processing.registry_update.exit_churn_limit
  apply exists_imp_spec
  obtain ⟨r1, hr1, h1⟩ := spec_imp_exists (safe_div_spec total_active_balance c.churn_limit_quotient)
  rw [hr1]
  cases r1 with
  | Err e =>
    obtain ⟨rfl, hz⟩ := h1.2 e rfl
    rw [hc.churn_limit_quotient] at hz
    refine ⟨_, rfl, ?_⟩
    simp [Spec.get_exit_churn_limit, Spec.uint64Div, hz, absResult, absArithError,
      
      
      except_error_bind, throw, throwThe, MonadExceptOf.throw]
  | Ok q =>
  obtain ⟨hq, hnz⟩ := h1.1 q rfl
  rw [hc.churn_limit_quotient] at hq hnz
  simp only [core.result.Result.Insts.CoreOpsTry.branch, bind_tc_ok]
  have hmax := core.cmp.impls.OrdU64.max_val c.min_per_epoch_churn_limit q
  simp only [core.cmp.max, core.cmp.OrdU64, liftFun2, bind_tc_ok]
  obtain ⟨r2, hr2, h2⟩ := spec_imp_exists (safe_rem_spec
    (core.cmp.impls.OrdU64.max c.min_per_epoch_churn_limit q) c.effective_balance_increment)
  rw [hr2]
  cases r2 with
  | Err e =>
    obtain ⟨rfl, hz⟩ := h2.2 e rfl
    rw [hc.effective_balance_increment] at hz
    refine ⟨_, rfl, ?_⟩
    simp [Spec.get_exit_churn_limit, Spec.uint64Div, Spec.uint64Mod, hnz, ← hq, hz,
      absResult, absArithError, except_ok_bind, except_error_bind, throw, throwThe,
      MonadExceptOf.throw, Pure.pure, Except.pure, 
      
      core.convert.FromSame.from]
  | Ok m =>
  obtain ⟨hm, hmnz⟩ := h2.1 m rfl
  rw [hc.effective_balance_increment] at hm hmnz
  simp only [bind_tc_ok]
  obtain ⟨r3, hr3, h3⟩ := spec_imp_exists (safe_sub_spec
    (core.cmp.impls.OrdU64.max c.min_per_epoch_churn_limit q) m)
  rw [hr3]
  cases r3 with
  | Err e =>
    obtain ⟨_, hlt⟩ := h3.2 e rfl
    rw [hm] at hlt
    exact absurd hlt (Nat.not_lt.mpr (Nat.mod_le _ _))
  | Ok d =>
  obtain ⟨hd, _⟩ := h3.1 d rfl
  simp only [bind_tc_ok, core.cmp.min,
    liftFun2]
  refine ⟨_, rfl, ?_⟩
  have hdb : d.val ≤ 18446744073709551615 := by have := d.hBounds; simp at this; omega
  have hmin := core.cmp.impls.OrdU64.min_val c.exit_churn_cap d
  rw [hc.exit_churn_cap, Nat.min_eq_right hdb] at hmin
  have hle : max p.MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA q.val % p.EFFECTIVE_BALANCE_INCREMENT
      ≤ max p.MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA q.val := Nat.mod_le _ _
  simp [Spec.get_exit_churn_limit, Spec.uint64Div, Spec.uint64Mod, Spec.uint64Sub, hnz, ← hq,
    hmnz, absResult, except_ok_bind, Pure.pure, Except.pure, hmin, hd, hm, hmax,
    hc.min_per_epoch_churn_limit]
  intro h
  rcases Nat.le_total p.MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA q.val with hq' | hq'
  · rw [Nat.max_eq_right hq'] at hle ⊢
    exact hle
  · rw [Nat.max_eq_left hq'] at h hle
    exact absurd h (Nat.not_lt.mpr hle)

theorem ceil_div_mul_ge (x c : Nat) (hc : 0 < c) : x ≤ ((x - 1) / c + 1) * c := by
  have h1 := Nat.div_add_mod (x - 1) c
  have h2 := Nat.mod_lt (x - 1) hc
  rw [Nat.mul_comm] at h1
  rw [Nat.add_mul, Nat.one_mul]
  omega

theorem compute_exit_epoch_and_update_churn_equiv (p : Spec.Preset)
    (exit_balance current_epoch earliest_exit_epoch exit_balance_to_consume total_active_balance : U64)
    (c : LhRegistryConstants) (hc : ConstantsMatch p c) :
    per_epoch_processing.registry_update.compute_exit_epoch_and_update_churn exit_balance
      current_epoch earliest_exit_epoch exit_balance_to_consume total_active_balance c ⦃ r =>
        absResult (fun x => (x.1.val, x.2.1.val, x.2.2.val)) r =
          Spec.exitChurnStep p total_active_balance.val current_epoch.val exit_balance.val
            earliest_exit_epoch.val exit_balance_to_consume.val ⦄ := by
  unfold per_epoch_processing.registry_update.compute_exit_epoch_and_update_churn
  apply exists_imp_spec
  obtain ⟨r1, hr1, h1⟩ := spec_imp_exists (compute_activation_exit_epoch_equiv p current_epoch
    c.max_seed_lookahead hc.max_seed_lookahead)
  rw [hr1]
  cases r1 with
  | Err e =>
    simp only [absResult] at h1
    refine ⟨_, rfl, ?_⟩
    simp only [Spec.exitChurnStep, ← h1]
    simp [absResult, absArithError, except_ok_bind, except_error_bind, Pure.pure, Except.pure,
      throw, throwThe, MonadExceptOf.throw, 
      
      core.convert.FromSame.from, Spec.uint64Add, Spec.uint64Sub,
      Spec.uint64Div, Spec.uint64Mul, Spec.UINT64_SIZE]
  | Ok act =>
  simp only [absResult] at h1
  simp only [core.result.Result.Insts.CoreOpsTry.branch, bind_tc_ok, core.cmp.max,
    liftFun2]
  have hmax := core.cmp.impls.OrdU64.max_val earliest_exit_epoch act
  obtain ⟨r2, hr2, h2⟩ := spec_imp_exists (exit_churn_limit_equiv p total_active_balance c hc)
  rw [hr2]
  cases r2 with
  | Err e =>
    simp only [absResult] at h2
    refine ⟨_, rfl, ?_⟩
    simp only [Spec.exitChurnStep, ← h1, ← h2]
    simp [absResult, absArithError, except_ok_bind, except_error_bind, Pure.pure, Except.pure,
      throw, throwThe, MonadExceptOf.throw, 
      
      core.convert.FromSame.from, Spec.uint64Add, Spec.uint64Sub,
      Spec.uint64Div, Spec.uint64Mul, Spec.UINT64_SIZE]
  | Ok churn =>
  simp only [absResult] at h2
  simp only [bind_tc_ok]
  have hone : (1#u64 : U64).val = 1 := rfl
  generalize hebtc_def : (if earliest_exit_epoch < core.cmp.impls.OrdU64.max earliest_exit_epoch act
    then churn else exit_balance_to_consume) = ebtc
  have hebtc : ebtc.val = (if earliest_exit_epoch.val < max earliest_exit_epoch.val act.val
      then churn.val else exit_balance_to_consume.val) := by
    rw [← hebtc_def]; split <;> rename_i h <;> simp [UScalar.lt_equiv, hmax] at h ⊢ <;> simp [h]
  have hif : (if earliest_exit_epoch < core.cmp.impls.OrdU64.max earliest_exit_epoch act
      then ok churn else ok exit_balance_to_consume) = (ok ebtc : Result U64) := by
    rw [← hebtc_def]; split <;> rfl
  rw [hif, bind_tc_ok]
  by_cases hgt : ebtc.val < exit_balance.val
  · have hgt' : exit_balance > ebtc := by simp [UScalar.lt_equiv]; exact hgt
    rw [if_pos hgt']
    obtain ⟨r3, hr3, h3⟩ := spec_imp_exists (safe_sub_spec exit_balance ebtc)
    rw [hr3]
    cases r3 with
    | Err e =>
      obtain ⟨rfl, hbad3⟩ := h3.2 e rfl
      try simp only [hmax, hone] at hbad3
      exfalso
      omega
    | Ok d =>
    obtain ⟨hv3, hle3⟩ := h3.1 d rfl
    try simp only [hmax, hone] at hv3 hle3
    simp only [bind_tc_ok]
    obtain ⟨r4, hr4, h4⟩ := spec_imp_exists (safe_sub_spec d 1#u64)
    rw [hr4]
    cases r4 with
    | Err e =>
      obtain ⟨rfl, hbad4⟩ := h4.2 e rfl
      try simp only [hv3, hone] at hbad4
      exfalso
      omega
    | Ok d1 =>
    obtain ⟨hv4, hle4⟩ := h4.1 d1 rfl
    try simp only [hv3, hone] at hv4 hle4
    simp only [bind_tc_ok]
    obtain ⟨r5, hr5, h5⟩ := spec_imp_exists (safe_div_spec d1 churn)
    rw [hr5]
    cases r5 with
    | Err e =>
      obtain ⟨rfl, hbad5⟩ := h5.2 e rfl
      try simp only [hv3, hv4, hmax, hone] at hbad5
      refine ⟨_, rfl, ?_⟩
      simp only [Spec.exitChurnStep, ← h1, ← h2, except_ok_bind, ← hebtc]
      simp [absResult, absArithError, except_ok_bind, except_error_bind, Pure.pure, Except.pure,
        throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.uint64Sub, Spec.uint64Div,
        Spec.uint64Mul, Spec.UINT64_SIZE, hgt, hle3, hle4, hbad5]
    | Ok q =>
    obtain ⟨hv5, hnz5⟩ := h5.1 q rfl
    try simp only [hv4] at hv5 hnz5
    simp only [bind_tc_ok]
    obtain ⟨r6, hr6, h6⟩ := spec_imp_exists (safe_add_spec q 1#u64)
    rw [hr6]
    cases r6 with
    | Err e =>
      obtain ⟨rfl, hbad6⟩ := h6.2 e rfl
      try simp only [hv5, hone] at hbad6
      refine ⟨_, rfl, ?_⟩
      simp only [Spec.exitChurnStep, ← h1, ← h2, except_ok_bind, ← hebtc]
      simp [absResult, absArithError, except_ok_bind, except_error_bind, Pure.pure, Except.pure,
        throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.uint64Sub, Spec.uint64Div,
        Spec.uint64Mul, Spec.UINT64_SIZE, hgt, hle3, hle4, hnz5, hbad6]
    | Ok extra =>
    obtain ⟨hv6, hfit6⟩ := h6.1 extra rfl
    try simp only [hv5, hone] at hv6 hfit6
    simp only [bind_tc_ok]
    obtain ⟨r7, hr7, h7⟩ := spec_imp_exists (safe_add_spec (core.cmp.impls.OrdU64.max earliest_exit_epoch act) extra)
    rw [hr7]
    cases r7 with
    | Err e =>
      obtain ⟨rfl, hbad7⟩ := h7.2 e rfl
      try simp only [hv6, hmax] at hbad7
      refine ⟨_, rfl, ?_⟩
      simp only [Spec.exitChurnStep, ← h1, ← h2, except_ok_bind, ← hebtc]
      simp [absResult, absArithError, except_ok_bind, except_error_bind, Pure.pure, Except.pure,
        throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.uint64Sub, Spec.uint64Div,
        Spec.uint64Mul, Spec.UINT64_SIZE, hgt, hle3, hle4, hnz5, hfit6, hbad7]
    | Ok newe =>
    obtain ⟨hv7, hfit7⟩ := h7.1 newe rfl
    try simp only [hv6, hmax] at hv7 hfit7
    simp only [bind_tc_ok]
    obtain ⟨r8, hr8, h8⟩ := spec_imp_exists (safe_mul_spec extra churn)
    rw [hr8]
    cases r8 with
    | Err e =>
      obtain ⟨rfl, hbad8⟩ := h8.2 e rfl
      try simp only [hv6] at hbad8
      refine ⟨_, rfl, ?_⟩
      simp only [Spec.exitChurnStep, ← h1, ← h2, except_ok_bind, ← hebtc]
      simp [absResult, absArithError, except_ok_bind, except_error_bind, Pure.pure, Except.pure,
        throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.uint64Sub, Spec.uint64Div,
        Spec.uint64Mul, Spec.UINT64_SIZE, hgt, hle3, hle4, hnz5, hfit6, hfit7, hbad8]
    | Ok m =>
    obtain ⟨hv8, hfit8⟩ := h8.1 m rfl
    try simp only [hv6] at hv8 hfit8
    simp only [bind_tc_ok]
    obtain ⟨r9, hr9, h9⟩ := spec_imp_exists (safe_add_spec ebtc m)
    rw [hr9]
    cases r9 with
    | Err e =>
      obtain ⟨rfl, hbad9⟩ := h9.2 e rfl
      try simp only [hv8] at hbad9
      refine ⟨_, rfl, ?_⟩
      simp only [Spec.exitChurnStep, ← h1, ← h2, except_ok_bind, ← hebtc]
      simp [absResult, absArithError, except_ok_bind, except_error_bind, Pure.pure, Except.pure,
        throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.uint64Sub, Spec.uint64Div,
        Spec.uint64Mul, Spec.UINT64_SIZE, hgt, hle3, hle4, hnz5, hfit6, hfit7, hfit8, hbad9]
    | Ok b =>
    obtain ⟨hv9, hfit9⟩ := h9.1 b rfl
    try simp only [hv8] at hv9 hfit9
    simp only [bind_tc_ok]
    obtain ⟨r10, hr10, h10⟩ := spec_imp_exists (safe_sub_spec b exit_balance)
    rw [hr10]
    cases r10 with
    | Err e =>
      obtain ⟨rfl, hbad10⟩ := h10.2 e rfl
      try simp only [hv9] at hbad10
      exfalso
      have key := ceil_div_mul_ge (exit_balance.val - ebtc.val) churn.val
        (Nat.pos_of_ne_zero hnz5)
      omega
    | Ok fin =>
    obtain ⟨hv10, hle10⟩ := h10.1 fin rfl
    try simp only [hv9] at hv10 hle10
    simp only [bind_tc_ok]
    refine ⟨_, rfl, ?_⟩
    simp only [Spec.exitChurnStep, ← h1, ← h2, except_ok_bind, ← hebtc]
    simp [absResult, except_ok_bind, Pure.pure, Except.pure,
          throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.uint64Sub, Spec.uint64Div,
          Spec.uint64Mul, Spec.UINT64_SIZE, hgt, hle3, hle4, hnz5, hfit6, hv7, hfit7, hfit8, hfit9, hv10, hle10]
  · have hgt' : ¬ exit_balance > ebtc := by simp [UScalar.lt_equiv]; omega
    rw [if_neg hgt']
    obtain ⟨r3, hr3, h3⟩ := spec_imp_exists (safe_sub_spec ebtc exit_balance)
    rw [hr3]
    cases r3 with
    | Err e =>
      obtain ⟨rfl, hbad3⟩ := h3.2 e rfl
      exfalso
      omega
    | Ok fin =>
    obtain ⟨hv3, hle3⟩ := h3.1 fin rfl
    simp only [bind_tc_ok]
    refine ⟨_, rfl, ?_⟩
    simp only [Spec.exitChurnStep, ← h1, ← h2, except_ok_bind, ← hebtc]
    simp [absResult, except_ok_bind, Pure.pure, Except.pure,
          Spec.uint64Sub, 
          hmax, hgt, hv3, hle3]

theorem eligibility_step_equiv (p : Spec.Preset) (fields : LhRegistryFields)
    (effective_balance current_epoch : U64) (c : LhRegistryConstants) (hc : ConstantsMatch p c) :
    per_epoch_processing.registry_update.eligibility_step fields effective_balance current_epoch c
      ⦃ r => absResult absRegistry r =
        Spec.eligibilityStep p current_epoch.val effective_balance.val (absRegistry fields) ⦄ := by
  unfold per_epoch_processing.registry_update.eligibility_step
  apply exists_imp_spec
  have hone : (1#u64 : U64).val = 1 := rfl
  by_cases hfar : fields.activation_eligibility_epoch.val = Spec.FAR_FUTURE_EPOCH
  swap
  · have hfar' : fields.activation_eligibility_epoch ≠ c.far_future_epoch := by
      intro h; apply hfar; rw [h, hc.far_future_epoch]
    refine ⟨.Ok fields, by simp [hfar'], ?_⟩
    simp [Spec.eligibilityStep, absRegistry, hfar, absResult, Pure.pure, Except.pure]
  have hfar' : fields.activation_eligibility_epoch = c.far_future_epoch := by
    rw [UScalar.eq_equiv, hfar, hc.far_future_epoch]
  by_cases hmin : p.MIN_ACTIVATION_BALANCE ≤ effective_balance.val
  swap
  · have hmin' : ¬ effective_balance >= c.min_activation_balance := by
      rw [ge_iff_le, UScalar.le_equiv, hc.min_activation_balance]; exact hmin
    refine ⟨.Ok fields, by simp [hfar', hmin'], ?_⟩
    simp [Spec.eligibilityStep, absRegistry, hfar, hmin, absResult, Pure.pure, Except.pure]
  have hmin' : effective_balance >= c.min_activation_balance := by
    rw [ge_iff_le, UScalar.le_equiv, hc.min_activation_balance]; exact hmin
  simp only [hfar', hmin', if_true]
  obtain ⟨r1, hr1, h1⟩ := spec_imp_exists (safe_add_spec current_epoch 1#u64)
  rw [hr1]
  cases r1 with
  | Err e =>
    obtain ⟨rfl, hov⟩ := h1.2 e rfl
    simp only [hone] at hov
    refine ⟨_, rfl, ?_⟩
    simp [Spec.eligibilityStep, hfar, hmin, hov, absResult, absArithError, absRegistry, except_error_bind, Pure.pure,
      Except.pure, throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.UINT64_SIZE,
      
      
      core.convert.FromSame.from]
  | Ok v =>
    obtain ⟨hv, hfit⟩ := h1.1 v rfl
    simp only [hone] at hv hfit
    refine ⟨_, rfl, ?_⟩
    simp [Spec.eligibilityStep, hfar, hmin, hfit, hv, absResult, absRegistry, except_ok_bind, Pure.pure,
      Except.pure, Spec.uint64Add, Spec.UINT64_SIZE,
      
      
      ]

theorem activation_step_equiv (p : Spec.Preset) (fields : LhRegistryFields)
    (current_epoch finalized_epoch : U64) (c : LhRegistryConstants) (hc : ConstantsMatch p c) :
    per_epoch_processing.registry_update.activation_step fields current_epoch finalized_epoch c
      ⦃ r => absResult absRegistry r =
        Spec.activationStep p current_epoch.val finalized_epoch.val (absRegistry fields) ⦄ := by
  unfold per_epoch_processing.registry_update.activation_step
  apply exists_imp_spec
  by_cases hfin : fields.activation_eligibility_epoch.val ≤ finalized_epoch.val
  swap
  · have hfin' : ¬ fields.activation_eligibility_epoch <= finalized_epoch := by
      rw [UScalar.le_equiv]; exact hfin
    refine ⟨.Ok fields, by simp [hfin'], ?_⟩
    simp [Spec.activationStep, absRegistry, hfin, absResult, Pure.pure, Except.pure]
  have hfin' : fields.activation_eligibility_epoch <= finalized_epoch := by
    rw [UScalar.le_equiv]; exact hfin
  by_cases hfar : fields.activation_epoch.val = Spec.FAR_FUTURE_EPOCH
  swap
  · have hfar' : fields.activation_epoch ≠ c.far_future_epoch := by
      intro h; apply hfar; rw [h, hc.far_future_epoch]
    refine ⟨.Ok fields, by simp [hfin', hfar'], ?_⟩
    simp [Spec.activationStep, absRegistry, hfin, hfar, absResult, Pure.pure, Except.pure]
  have hfar' : fields.activation_epoch = c.far_future_epoch := by
    rw [UScalar.eq_equiv, hfar, hc.far_future_epoch]
  simp only [hfin', hfar', if_true]
  obtain ⟨r1, hr1, h1⟩ := spec_imp_exists (compute_activation_exit_epoch_equiv p current_epoch
    c.max_seed_lookahead hc.max_seed_lookahead)
  rw [hr1]
  simp only [absResult] at h1
  cases r1 with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    simp only [Spec.activationStep, absRegistry, hfin, hfar, ← h1]
    simp [absResult, absArithError, except_error_bind, Pure.pure,
      Except.pure, 
      
      
      core.convert.FromSame.from]
  | Ok v =>
    refine ⟨_, rfl, ?_⟩
    simp only [Spec.activationStep, absRegistry, hfin, hfar, ← h1]
    simp [absResult, absRegistry, except_ok_bind, Pure.pure,
      Except.pure, 
      
      
      ]

theorem ejection_step_equiv (p : Spec.Preset) (fields : LhRegistryFields)
    (effective_balance current_epoch total_active_balance : U64) (c : LhRegistryConstants)
    (hc : ConstantsMatch p c) :
    per_epoch_processing.registry_update.ejection_step fields effective_balance current_epoch
      total_active_balance c ⦃ r => absResult absRegistry r =
        Spec.ejectionStep p total_active_balance.val current_epoch.val effective_balance.val
          (absRegistry fields) ⦄ := by
  unfold per_epoch_processing.registry_update.ejection_step
  apply exists_imp_spec
  by_cases hact : fields.activation_epoch.val ≤ current_epoch.val
      ∧ current_epoch.val < fields.exit_epoch.val
      ∧ effective_balance.val ≤ p.EJECTION_BALANCE
      ∧ fields.exit_epoch.val = Spec.FAR_FUTURE_EPOCH
  swap
  · refine ⟨.Ok fields, ?_, ?_⟩
    · by_cases h1 : fields.activation_epoch.val ≤ current_epoch.val
      · by_cases h2 : current_epoch.val < fields.exit_epoch.val
        · by_cases h3 : effective_balance.val ≤ p.EJECTION_BALANCE
          · have h4 : fields.exit_epoch ≠ c.far_future_epoch := by
              intro h; exact hact ⟨h1, h2, h3, by rw [h, hc.far_future_epoch]⟩
            simp [UScalar.le_equiv, UScalar.lt_equiv, h1, h2, h3, h4, hc.ejection_balance]
          · simp [UScalar.le_equiv, UScalar.lt_equiv, h1, h2, h3, hc.ejection_balance]
        · simp [UScalar.le_equiv, UScalar.lt_equiv, h1, h2]
      · simp [UScalar.le_equiv, h1]
    · by_cases hcond : (fields.activation_epoch.val ≤ current_epoch.val
          ∧ current_epoch.val < fields.exit_epoch.val)
          ∧ effective_balance.val ≤ p.EJECTION_BALANCE
      · have hnfar : fields.exit_epoch.val ≠ Spec.FAR_FUTURE_EPOCH :=
          fun h => hact ⟨hcond.1.1, hcond.1.2, hcond.2, h⟩
        simp [Spec.ejectionStep, Spec.exitStep, absRegistry, hcond.1.1, hcond.1.2, hcond.2,
          hnfar, absResult, Pure.pure, Except.pure]
      · have : ((decide (fields.activation_epoch.val ≤ current_epoch.val)
            && decide (current_epoch.val < fields.exit_epoch.val))
            && decide (effective_balance.val ≤ p.EJECTION_BALANCE)) = false := by
          simp only [Bool.and_eq_false_iff, decide_eq_false_iff_not]
          by_cases h1 : fields.activation_epoch.val ≤ current_epoch.val
          · by_cases h2 : current_epoch.val < fields.exit_epoch.val
            · exact Or.inr (fun h3 => hcond ⟨⟨h1, h2⟩, h3⟩)
            · exact Or.inl (Or.inr h2)
          · exact Or.inl (Or.inl h1)
        simp only [Spec.ejectionStep, absRegistry]
        simp [absResult, absRegistry, Pure.pure, Except.pure]
        intro h1 h2 h3
        exact absurd ⟨⟨h1, h2⟩, h3⟩ hcond
  obtain ⟨h1, h2, h3, h4⟩ := hact
  have h4' : fields.exit_epoch = c.far_future_epoch := by
    rw [UScalar.eq_equiv, h4, hc.far_future_epoch]
  have hA : fields.activation_epoch <= current_epoch := by rw [UScalar.le_equiv]; exact h1
  have hB : current_epoch < fields.exit_epoch := by rw [UScalar.lt_equiv]; exact h2
  have hC : effective_balance <= c.ejection_balance := by
    rw [UScalar.le_equiv, hc.ejection_balance]; exact h3
  simp only [hA, hB, hC, decide_true, if_true, bind_tc_ok]
  rw [if_pos h4']
  have hcond : (decide (fields.activation_epoch.val ≤ current_epoch.val)
      && decide (current_epoch.val < fields.exit_epoch.val)
      && decide (effective_balance.val ≤ p.EJECTION_BALANCE)) = true := by
    simp [h1, h2, h3]
  have hcur : current_epoch.val < Spec.FAR_FUTURE_EPOCH := by rw [← h4]; exact h2
  obtain ⟨r1, hr1, hx⟩ := spec_imp_exists (compute_exit_epoch_and_update_churn_equiv p
    effective_balance current_epoch fields.earliest_exit_epoch fields.exit_balance_to_consume
    total_active_balance c hc)
  rw [hr1]
  simp only [absResult] at hx
  cases r1 with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    simp only [Spec.ejectionStep, Spec.exitStep, absRegistry, h4, ← hx]
    simp [absResult, absArithError, except_ok_bind, except_error_bind, Pure.pure,
      Except.pure, throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.UINT64_SIZE,
      
      
      core.convert.FromSame.from]
    exact ⟨h1, hcur, h3⟩
  | Ok x =>
  obtain ⟨exit_epoch, earliest, ebtc⟩ := x
  simp only [core.result.Result.Insts.CoreOpsTry.branch, bind_tc_ok, uncurry_apply_pair]
  obtain ⟨r2, hr2, h2'⟩ := spec_imp_exists (safe_add_spec exit_epoch
    c.min_validator_withdrawability_delay)
  rw [hr2]
  rw [hc.min_validator_withdrawability_delay] at h2'
  cases r2 with
  | Err e =>
    obtain ⟨rfl, hov⟩ := h2'.2 e rfl
    refine ⟨_, rfl, ?_⟩
    simp only [Spec.ejectionStep, Spec.exitStep, absRegistry, h4, ← hx]
    simp [absResult, absArithError, except_ok_bind, except_error_bind, Pure.pure,
      Except.pure, throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.UINT64_SIZE,
      
      
      core.convert.FromSame.from, hov, h1, h3, hcur]
  | Ok w =>
    obtain ⟨hw, hfit⟩ := h2'.1 w rfl
    refine ⟨_, rfl, ?_⟩
    simp only [Spec.ejectionStep, Spec.exitStep, absRegistry, h4, ← hx]
    simp [absResult, absRegistry, except_ok_bind, Pure.pure,
      Except.pure, throw, throwThe, MonadExceptOf.throw, Spec.uint64Add, Spec.UINT64_SIZE,
      
      
      hfit, hw, h1, h3, hcur]

theorem registry_update_equiv (p : Spec.Preset) (fields : LhRegistryFields)
    (effective_balance current_epoch finalized_epoch total_active_balance : U64)
    (c : LhRegistryConstants) (hc : ConstantsMatch p c) :
    per_epoch_processing.registry_update.registry_update fields effective_balance current_epoch
      finalized_epoch total_active_balance c ⦃ r => absResult absRegistry r =
        Spec.registryStepIndependent p total_active_balance.val current_epoch.val
          finalized_epoch.val effective_balance.val (absRegistry fields) ⦄ := by
  unfold per_epoch_processing.registry_update.registry_update
  apply exists_imp_spec
  obtain ⟨r1, hr1, h1⟩ := spec_imp_exists (eligibility_step_equiv p fields effective_balance
    current_epoch c hc)
  rw [hr1]
  simp only [absResult] at h1
  cases r1 with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    simp only [Spec.registryStepIndependent, ← h1]
    simp [absResult, absArithError, except_error_bind,
      
      
      core.convert.FromSame.from]
  | Ok f1 =>
  simp only [core.result.Result.Insts.CoreOpsTry.branch, bind_tc_ok]
  obtain ⟨r2, hr2, h2⟩ := spec_imp_exists (ejection_step_equiv p f1 effective_balance
    current_epoch total_active_balance c hc)
  rw [hr2]
  simp only [absResult] at h2
  cases r2 with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    simp only [Spec.registryStepIndependent, ← h1, except_ok_bind, ← h2]
    simp [absResult, absArithError, except_error_bind,
      
      
      core.convert.FromSame.from]
  | Ok f2 =>
  simp only [bind_tc_ok]
  obtain ⟨r3, hr3, h3⟩ := spec_imp_exists (activation_step_equiv p f2 current_epoch
    finalized_epoch c hc)
  refine ⟨r3, hr3, ?_⟩
  simp only [Spec.registryStepIndependent, ← h1, except_ok_bind, ← h2]
  exact h3

/-- On valid states, Lighthouse's registry update equals the spec's loop body for one
validator. -/
theorem registry_update_eq_spec (p : Spec.Preset) (fields : LhRegistryFields)
    (effective_balance current_epoch finalized_epoch total_active_balance : U64)
    (c : LhRegistryConstants) (hc : ConstantsMatch p c) (activation_epoch : Nat)
    (hbalances : p.EJECTION_BALANCE < p.MIN_ACTIVATION_BALANCE)
    (hfinalized : finalized_epoch.val ≤ current_epoch.val)
    (hcurrent : current_epoch.val + 1 < Spec.UINT64_SIZE)
    (hactivation : Spec.compute_activation_exit_epoch p current_epoch.val = .ok activation_epoch) :
    per_epoch_processing.registry_update.registry_update fields effective_balance current_epoch
      finalized_epoch total_active_balance c ⦃ r => absResult absRegistry r =
        Spec.registryStepExclusive p total_active_balance.val current_epoch.val
          finalized_epoch.val activation_epoch effective_balance.val (absRegistry fields) ⦄ := by
  apply WP.spec_mono (registry_update_equiv p fields effective_balance current_epoch
    finalized_epoch total_active_balance c hc)
  intro r hr
  rw [hr]
  apply Spec.registryStepIndependent_eq_exclusive p _ _ _ _ _ _ hbalances hfinalized _ hcurrent
    hactivation
  have hb : fields.exit_epoch.val ≤ 18446744073709551615 := by
    have := fields.exit_epoch.hBounds; simp at this; omega
  simpa [absRegistry, Spec.FAR_FUTURE_EPOCH] using hb

end EpochProofs
