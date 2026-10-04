import EpochProofs.Sanity.Invariants.LighthouseTailDeposits

/-!
# The deposit views that Lighthouse predicts

Lighthouse decides the pending deposits before its single pass, on at most
`MAX_PENDING_DEPOSITS_PER_EPOCH` deposits, with statuses that it predicts from the state before
the registry update. The spec decides after the registry update, on the whole queue. This file
shows that the two inputs give the same decisions.
-/

namespace EpochProofs.Spec

/-! ## Only the first `MAX_PENDING_DEPOSITS_PER_EPOCH` deposits matter -/

/-- Two loop results agree on what `depositDecisions` reads: the loop state and the churn flag. -/
def LoopAgree : SpecM DepositLoopResult → SpecM DepositLoopResult → Prop
  | .ok r1, .ok r2 => r1.state = r2.state ∧ r1.churn_reached = r2.churn_reached
  | .error e1, .error e2 => e1 = e2
  | _, _ => False

/-- From index `k ≤ MAX`, the loop over all views agrees with the loop over the first `MAX - k`
views: at index `MAX` the loop stops with the state unchanged and no churn flag. -/
theorem depositLoop_take_agree (fs avail : Nat) :
    ∀ (vs : List DepositSpecView) (ls : DepositLoopState),
      ls.next_deposit_index ≤ Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH →
      LoopAgree (depositLoop Preset.mainnet fs avail ls vs)
        (depositLoop Preset.mainnet fs avail ls
          (vs.take (Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH - ls.next_deposit_index)))
  | [], ls, _ => by simp [depositLoop, LoopAgree, pure, Except.pure]
  | v :: vs, ls, hk => by
    by_cases hm : ls.next_deposit_index = Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH
    · rw [hm, Nat.sub_self, List.take_zero]
      have hs : depositStep Preset.mainnet fs avail ls v = .ok ⟨ls, true, false⟩ := by
        unfold depositStep
        have : (decide (ls.next_deposit_index ≥ Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH))
            = true := by simp [hm]
        simp [this, pure, Except.pure]
      simp [depositLoop, hs, LoopAgree, bind, Except.bind, pure, Except.pure]
    · have hlt : ls.next_deposit_index < Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH :=
        Nat.lt_of_le_of_ne hk hm
      obtain ⟨m, hmk⟩ : ∃ m, Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH -
          ls.next_deposit_index = m + 1 :=
        ⟨_, (Nat.succ_pred_eq_of_pos (Nat.sub_pos_of_lt hlt)).symm⟩
      rw [hmk, List.take_succ_cons]
      simp only [depositLoop, bind, Except.bind]
      cases hs : depositStep Preset.mainnet fs avail ls v with
      | error e => simp [LoopAgree]
      | ok r =>
        rcases depositStep_cases fs avail ls v r hs with ⟨hst, -⟩ | ⟨hst, -, hidx, -⟩
        · simp [hst, LoopAgree, pure, Except.pure]
        · simp only [hst, Bool.false_eq_true, if_false]
          have hk' : r.state.next_deposit_index ≤
              Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH := by
            rw [hidx]; exact hlt
          have hm' : m = Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH -
              r.state.next_deposit_index := by rw [hidx, Nat.sub_succ, hmk]; rfl
          rw [hm']
          exact depositLoop_take_agree fs avail vs r.state hk'

/-- The decisions read only the first `MAX_PENDING_DEPOSITS_PER_EPOCH` views. -/
theorem depositDecisions_take_max (fs dbtc churn : Nat) (views : List DepositSpecView) :
    depositDecisions Preset.mainnet fs dbtc churn views =
      depositDecisions Preset.mainnet fs dbtc churn
        (views.take Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH) := by
  unfold depositDecisions
  cases uint64Add dbtc churn with
  | error e => rfl
  | ok avail =>
    simp only [bind, Except.bind]
    have h := depositLoop_take_agree fs avail views ⟨0, 0, []⟩ (Nat.zero_le _)
    simp only [Nat.sub_zero] at h
    revert h
    cases depositLoop Preset.mainnet fs avail ⟨0, 0, []⟩ views <;>
      cases depositLoop Preset.mainnet fs avail ⟨0, 0, []⟩
        (views.take Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH) <;>
      intro h <;> simp only [LoopAgree] at h
    · rw [h]
    · obtain ⟨h1, h2⟩ := h
      dsimp only
      rw [h1, h2]

/-! ## The registry part of one Lighthouse row step -/

/-- The validator of a row after `lhRowStep` is the input validator with the registry fields of
`registryStepIndependent`. The inactivity and rewards steps do not change the validator. -/
theorem lhRowStep_registry (ctx : LhStepContext) (base : Gwei) (churn : Epoch × Gwei) (r : Row)
    (x : (Epoch × Gwei) × Row) (h : lhRowStep Preset.mainnet ctx base churn r = .ok x) :
    ∃ g, registryStepIndependent Preset.mainnet ctx.total_active_balance ctx.current_epoch
        ctx.finalized_epoch r.validator.effective_balance (registryFieldsOf r.validator churn) =
          .ok g ∧ x.2.validator = r.validator.withRegistryFields g := by
  unfold lhRowStep at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact ⟨_, ‹registryStepIndependent _ _ _ _ _ _ = _›, rfl⟩)

/-- `lhRowStep_registry` with the spec's exclusive registry step. -/
theorem lhRowStep_registry_exclusive (ctx : LhStepContext) (base : Gwei) (churn : Epoch × Gwei)
    (r : Row) (x : (Epoch × Gwei) × Row) (activation_epoch : Epoch)
    (hfinalized : ctx.finalized_epoch ≤ ctx.current_epoch)
    (hexit : r.validator.exit_epoch ≤ FAR_FUTURE_EPOCH)
    (hcurrent : ctx.current_epoch + 1 < UINT64_SIZE)
    (hactivation : compute_activation_exit_epoch Preset.mainnet ctx.current_epoch =
      .ok activation_epoch)
    (h : lhRowStep Preset.mainnet ctx base churn r = .ok x) :
    ∃ g, registryStepExclusive Preset.mainnet ctx.total_active_balance ctx.current_epoch
        ctx.finalized_epoch activation_epoch r.validator.effective_balance
          (registryFieldsOf r.validator churn) = .ok g ∧
      x.2.validator = r.validator.withRegistryFields g := by
  obtain ⟨g, hg, hx⟩ := lhRowStep_registry ctx base churn r x h
  refine ⟨g, ?_, hx⟩
  rw [← registryStepIndependent_eq_exclusive Preset.mainnet _ _ _ _ _ _ (by decide) hfinalized
    hexit hcurrent hactivation]
  exact hg

/-! ## Rows of a pass -/

/-- Each output row of a successful `passM` comes from the input row at the same position, by
one step from some accumulator. -/
theorem passM_rows {A R : Type} (f : A → R → SpecM (A × R)) :
    ∀ (rows : List R) (a : A) (y : A × List R), passM f a rows = .ok y →
      y.2.length = rows.length ∧
        ∀ i (h1 : i < rows.length) (h2 : i < y.2.length), ∃ a' x, f a' rows[i] = .ok x ∧
          x.2 = y.2[i]
  | [], a, y, h => by
    simp only [passM, pure, Except.pure, Except.ok.injEq] at h
    subst h
    exact ⟨rfl, fun i h1 => absurd h1 (Nat.not_lt_zero _)⟩
  | r :: rs, a, y, h => by
    rw [passM_cons] at h
    obtain ⟨x, hx, h⟩ := specM_bind_ok h
    obtain ⟨z, hz, h⟩ := specM_bind_ok h
    simp only [pure, Except.pure, Except.ok.injEq] at h
    subst h
    obtain ⟨hlen, hget⟩ := passM_rows f rs x.1 z hz
    refine ⟨by simp [hlen], fun i h1 h2 => ?_⟩
    cases i with
    | zero => exact ⟨a, x, hx, rfl⟩
    | succ i =>
      exact hget i (Nat.lt_of_succ_lt_succ h1) (Nat.lt_of_succ_lt_succ h2)

/-- After a successful Lighthouse pass over the rows of `s`, each validator is the validator of
`s` with the registry fields of a successful `registryStepExclusive`. -/
theorem passM_rows_registry (ctx : LhStepContext) (bf : Row → Gwei) (s : BeaconState)
    (churn0 churn : Epoch × Gwei) (rows : List Row) (activation_epoch : Epoch)
    (hfinalized : ctx.finalized_epoch ≤ ctx.current_epoch)
    (hexit : ∀ i (h : i < s.validators.length), s.validators[i].exit_epoch ≤ FAR_FUTURE_EPOCH)
    (hcurrent : ctx.current_epoch + 1 < UINT64_SIZE)
    (hactivation : compute_activation_exit_epoch Preset.mainnet ctx.current_epoch =
      .ok activation_epoch)
    (h : passM (fun c r => lhRowStep Preset.mainnet ctx (bf r) c r) churn0 (rowsOf s) =
      .ok (churn, rows)) :
    rows.length = s.validators.length ∧
      ∀ i (h1 : i < s.validators.length) (h2 : i < rows.length), ∃ c g,
        registryStepExclusive Preset.mainnet ctx.total_active_balance ctx.current_epoch
          ctx.finalized_epoch activation_epoch s.validators[i].effective_balance
            (registryFieldsOf s.validators[i] c) = .ok g ∧
        rows[i].validator = s.validators[i].withRegistryFields g := by
  obtain ⟨hlen, hget⟩ := passM_rows _ (rowsOf s) churn0 (churn, rows) h
  rw [rowsOf_length] at hlen
  refine ⟨hlen, fun i h1 h2 => ?_⟩
  have h1' : i < (rowsOf s).length := by rw [rowsOf_length]; exact h1
  obtain ⟨c, x, hx, hxr⟩ := hget i h1' h2
  have hrow := rowsOf_getElem s i h1'
  rw [hrow] at hx
  obtain ⟨g, hg, hv⟩ := lhRowStep_registry_exclusive ctx _ c _ x activation_epoch hfinalized
    (hexit i h1) hcurrent hactivation hx
  exact ⟨c, g, hg, by rw [← hxr, hv]⟩

/-! ## The predicted views equal the spec's views -/

/-- A successful `uint64Add` fits in a `u64`. -/
private theorem uint64Add_lt {a b c : Nat} (h : uint64Add a b = .ok c) : a + b < UINT64_SIZE := by
  unfold uint64Add at h
  split at h
  · assumption
  · cases h

/-- When `exitStep` sets an exit, the new withdrawable epoch, exit plus the delay, fits in a
`u64`. -/
theorem exitStep_exit_lt (p : Preset) (total_active_balance current_epoch : Nat)
    (effective_balance : Gwei) (f g : RegistryFields)
    (h : exitStep p total_active_balance current_epoch effective_balance f = .ok g)
    (hfar : f.exit_epoch = FAR_FUTURE_EPOCH) :
    g.exit_epoch + p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY < UINT64_SIZE := by
  unfold exitStep at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  split at h
  · rename_i hne
    simp [hfar] at hne
  · repeat' split at h
    all_goals first
      | contradiction
      | (cases h; exact uint64Add_lt ‹uint64Add _ _ = _›)

/-- On mainnet, a validator that the registry step ejects gets an exit epoch below
`FAR_FUTURE_EPOCH`: its withdrawable epoch, exit plus 256, is computed by a checked add. -/
theorem registryStepExclusive_ejected (total_active_balance current_epoch finalized_epoch
    activation_epoch : Nat) (effective_balance : Gwei) (f g : RegistryFields)
    (hreg : registryStepExclusive Preset.mainnet total_active_balance current_epoch
      finalized_epoch activation_epoch effective_balance f = .ok g)
    (hact : f.activation_epoch ≤ current_epoch) (hex : current_epoch < f.exit_epoch)
    (hej : effective_balance ≤ Preset.mainnet.EJECTION_BALANCE)
    (hfar : f.exit_epoch = FAR_FUTURE_EPOCH) : g.exit_epoch < FAR_FUTURE_EPOCH := by
  have hlow : ¬ Preset.mainnet.MIN_ACTIVATION_BALANCE ≤ effective_balance := by
    have : Preset.mainnet.EJECTION_BALANCE < Preset.mainnet.MIN_ACTIVATION_BALANCE := by decide
    exact fun hle => Nat.lt_irrefl _ (Nat.lt_of_lt_of_le (Nat.lt_of_lt_of_le this hle) hej)
  unfold registryStepExclusive at hreg
  split at hreg
  · rename_i hq
    simp only [Bool.and_eq_true, decide_eq_true_eq, ge_iff_le] at hq
    exact absurd hq.2 hlow
  · split at hreg
    · have h := exitStep_exit_lt _ _ _ _ f g hreg hfar
      have hd : Preset.mainnet.MIN_VALIDATOR_WITHDRAWABILITY_DELAY = 256 := rfl
      rw [hd] at h
      have h' : @LT.lt Nat _ (g.exit_epoch + 256) (2 ^ 64) := h
      show @LT.lt Nat _ g.exit_epoch (2 ^ 64 - 1)
      omega
    · rename_i _ hnej
      simp only [Bool.and_eq_true, decide_eq_true_eq, not_and] at hnej
      exact absurd hej (hnej ⟨hact, hex⟩)

/-- `predictedStatus` reads only the validator epochs, not the churn. -/
theorem predictedStatus_churn (v : Validator) (c c' : Epoch × Gwei) (eb current next : Nat) :
    predictedStatus Preset.mainnet true true (registryFieldsOf v c) eb current next =
      predictedStatus Preset.mainnet true true (registryFieldsOf v c') eb current next := rfl

/-- After a successful Lighthouse pass, the flags that the spec reads in a state with the new
validators equal the flags that Lighthouse predicts from the state before the pass. -/
theorem views_eq (ctx : LhStepContext) (bf : Row → Gwei) (s : BeaconState)
    (churn0 churn : Epoch × Gwei) (rows : List Row) (activation_epoch : Epoch)
    (hfinalized : ctx.finalized_epoch ≤ ctx.current_epoch)
    (hexit : ∀ i (h : i < s.validators.length), s.validators[i].exit_epoch ≤ FAR_FUTURE_EPOCH)
    (hvalid : ∀ v ∈ s.validators, v.exit_epoch = FAR_FUTURE_EPOCH →
      v.withdrawable_epoch = FAR_FUTURE_EPOCH)
    (hnext : ctx.current_epoch + 1 < FAR_FUTURE_EPOCH)
    (hactivation : compute_activation_exit_epoch Preset.mainnet ctx.current_epoch =
      .ok activation_epoch)
    (h : passM (fun c r => lhRowStep Preset.mainnet ctx (bf r) c r) churn0 (rowsOf s) =
      .ok (churn, rows))
    (T : BeaconState) (hT : T.validators = rows.map (·.validator)) (l : List PendingDeposit) :
    l.map (specDepositView T (ctx.current_epoch + 1)) =
      l.map (lhDepositView s ctx.current_epoch) := by
  have hcurrent : ctx.current_epoch + 1 < UINT64_SIZE := by
    have h' : @LT.lt Nat _ (ctx.current_epoch + 1) (2 ^ 64 - 1) := hnext
    show @LT.lt Nat _ (ctx.current_epoch + 1) (2 ^ 64)
    omega
  obtain ⟨hlen, hget⟩ := passM_rows_registry ctx bf s churn0 churn rows activation_epoch
    hfinalized hexit hcurrent hactivation h
  have hTlen : T.validators.length = s.validators.length := by rw [hT, List.length_map, hlen]
  have hTget : ∀ (i : Nat) (v : Validator), s.validators[i]? = some v → ∃ c g,
      T.validators[i]? = some (v.withRegistryFields g) ∧
      registryStepExclusive Preset.mainnet ctx.total_active_balance ctx.current_epoch
        ctx.finalized_epoch activation_epoch v.effective_balance (registryFieldsOf v c) =
          .ok g := by
    intro i v hv
    obtain ⟨h1, rfl⟩ := List.getElem?_eq_some_iff.mp hv
    have h2 : i < rows.length := by rw [hlen]; exact h1
    obtain ⟨c, g, hg, hrv⟩ := hget i h1 h2
    exact ⟨c, g, by simp [hT, List.getElem?_eq_getElem h2, hrv], hg⟩
  have hpk : T.validators.map (·.pubkey) = s.validators.map (·.pubkey) := by
    rw [hT, List.map_map]
    apply List.ext_getElem
    · simp [hlen]
    · intro i h1 h2
      have hi : i < s.validators.length := by simpa using h2
      have hr : i < rows.length := by simpa using h1
      obtain ⟨c, g, -, hrv⟩ := hget i hi hr
      simp only [List.getElem_map, Function.comp_def, hrv]
      rfl
  apply List.map_congr_left
  intro d _
  unfold specDepositView lhDepositView
  rw [hpk]
  cases hs : s.validators[(s.validators.map (·.pubkey)).idxOf d.pubkey]? with
  | none =>
    have hn : T.validators[(s.validators.map (·.pubkey)).idxOf d.pubkey]? = none := by
      rw [List.getElem?_eq_none_iff] at hs ⊢
      rw [hTlen]; exact hs
    rw [hn]
  | some v =>
    obtain ⟨c, g, hg, hreg⟩ := hTget _ v hs
    rw [hg]
    have hmem : v ∈ s.validators := List.mem_of_getElem? hs
    have hex : v.exit_epoch ≤ FAR_FUTURE_EPOCH := by
      obtain ⟨hk, rfl⟩ := List.getElem?_eq_some_iff.mp hs
      exact hexit _ hk
    have hpred := predictedStatus_eq_post_registry Preset.mainnet _ _ _ _ _ _ g hreg (by decide)
      hex (hvalid v hmem) hnext
      (fun ha hx he hf => registryStepExclusive_ejected _ _ _ _ _ _ g hreg ha hx he hf)
    dsimp only
    rw [predictedStatus_churn v (0, 0) c, hpred]
    rfl

end EpochProofs.Spec
