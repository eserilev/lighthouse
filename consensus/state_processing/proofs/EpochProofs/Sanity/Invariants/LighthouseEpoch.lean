import EpochProofs.Sanity.LighthouseSinglePass
import EpochProofs.Sanity.Invariants.EpochEnd
import EpochProofs.Sanity.Invariants.Lengths
import EpochProofs.Sanity.RowsSlashings
import EpochProofs.Sanity.RowsRegistry
import EpochProofs.Sanity.PendingDeposits
import EpochProofs.Sanity.Invariants.Reachable

/-!
# The Gloas epoch with the Lighthouse single pass

`process_epoch_lh` is `process_epoch` with one Lighthouse pass in place of the inactivity,
rewards, registry and slashings steps. On mainnet it gives the same `ok` results as
`process_epoch`, under facts about the state at the start of the epoch.
-/

namespace EpochProofs.Spec

/-! ## The total active balance inside the epoch -/

/-- What `get_total_active_balance` reads of one validator at epoch `epoch`. -/
def activeKey (epoch : Epoch) (v : Validator) : Gwei × Bool :=
  (v.effective_balance, is_active_validator v epoch)

/-- The active indices depend only on the activity of each validator. -/
private theorem active_indices_map (l : List Validator) (epoch : Epoch) :
    (l.zipIdx.filter fun x => is_active_validator x.1 epoch).map (·.2) =
      (((l.map (activeKey epoch)).zipIdx.filter fun x => x.1.2).map (·.2)) := by
  simp [activeKey, List.zipIdx_map, List.filter_map, Function.comp_def]

/-- Reading an effective balance depends only on the list of effective balances. -/
private theorem listGet_effective_balance {l l' : List Validator}
    (h : l'.map (·.effective_balance) = l.map (·.effective_balance)) (i : Nat) :
    (do pure (← listGet l' i).effective_balance : SpecM Gwei) =
      (do pure (← listGet l i).effective_balance) := by
  have hi := congrArg (·[i]?) h
  simp only [List.getElem?_map] at hi
  unfold listGet
  cases h1 : l'[i]? <;> cases h2 : l[i]? <;> simp_all [bind, Except.bind, pure, Except.pure]

/-- `get_total_active_balance` reads only the slot and the `activeKey` of each validator. -/
theorem get_total_active_balance_congr_key (p : Preset) (s s' : BeaconState) (epoch : Epoch)
    (hepoch : get_current_epoch p s = .ok epoch) (hslot : s'.slot = s.slot)
    (hkey : s'.validators.map (activeKey epoch) = s.validators.map (activeKey epoch)) :
    get_total_active_balance p s' = get_total_active_balance p s := by
  have hepoch' : get_current_epoch p s' = .ok epoch := by
    rw [← hepoch]; simp only [get_current_epoch, hslot]
  have heb : s'.validators.map (·.effective_balance) = s.validators.map (·.effective_balance) := by
    have := congrArg (List.map Prod.fst) hkey
    simpa [activeKey, Function.comp_def] using this
  unfold get_total_active_balance
  rw [hepoch, hepoch']
  show get_total_balance p s' (get_active_validator_indices s' epoch) =
    get_total_balance p s (get_active_validator_indices s epoch)
  have hf : (fun index => (do pure (← listGet s'.validators index).effective_balance :
      SpecM Gwei)) = (fun index => do pure (← listGet s.validators index).effective_balance) :=
    funext (listGet_effective_balance heb)
  unfold get_total_balance get_active_validator_indices
  rw [active_indices_map, active_indices_map, hkey, hf]

/-- If every step keeps a key of the row, a pass keeps the list of keys. -/
private theorem passM_keys {A R K : Type} (f : A → R → SpecM (A × R)) (key : R → K)
    (hf : ∀ a r x, f a r = .ok x → key x.2 = key r) :
    ∀ (rows : List R) (a : A) (y : A × List R), passM f a rows = .ok y →
      y.2.map key = rows.map key
  | [], a, y, h => by
    simp only [passM, pure, Except.pure, Except.ok.injEq] at h
    subst h
    rfl
  | r :: rs, a, y, h => by
    rw [passM_cons] at h
    obtain ⟨x, hx, h⟩ := specM_bind_ok h
    obtain ⟨z, hz, h⟩ := specM_bind_ok h
    simp only [pure, Except.pure, Except.ok.injEq] at h
    subst h
    simp only [List.map_cons, hf a r x hx, passM_keys f key hf rs x.1 z hz]

/-- One registry row step keeps the `activeKey` of the validator. The activation epoch is after
the current epoch, and an exit is after the current epoch. -/
theorem registryRowStep_activeKey (p : Preset) (total_active_balance current_epoch
    finalized_epoch activation_epoch : Nat) (hact : current_epoch < activation_epoch)
    (hcur : current_epoch < FAR_FUTURE_EPOCH) (churn : Epoch × Gwei) (r : Row)
    (x : (Epoch × Gwei) × Row)
    (h : registryRowStep p total_active_balance current_epoch finalized_epoch activation_epoch
      churn r = .ok x) :
    activeKey current_epoch x.2.validator = activeKey current_epoch r.validator := by
  unfold registryRowStep at h
  obtain ⟨f, hf, h⟩ := specM_bind_ok h
  simp only [pure, Except.pure, Except.ok.injEq] at h
  subst h
  simp only [activeKey, Validator.withRegistryFields, is_active_validator, Prod.mk.injEq,
    true_and]
  unfold registryStepExclusive at hf
  simp only [bind, Except.bind, pure, Except.pure] at hf
  split at hf
  · split at hf
    · cases hf
    · cases hf; rfl
  · split at hf
    · have ha := exitStep_activation _ _ _ _ _ _ hf
      rcases exitStep_cases _ _ _ _ _ _ hf with ⟨-, rfl⟩ | ⟨hfar, hge, -⟩
      · rfl
      · simp only [registryFieldsOf] at ha hfar
        have h2 : current_epoch < f.exit_epoch := by omega
        simp [ha.1, hfar, h2, hcur]
    · split at hf
      · rename_i hel
        cases hf
        simp only [registryFieldsOf, Bool.and_eq_true, decide_eq_true_eq, beq_iff_eq] at hel ⊢
        rw [hel.2]
        have h1 : ¬ FAR_FUTURE_EPOCH ≤ current_epoch := Nat.not_le.mpr hcur
        have h2 : ¬ activation_epoch ≤ current_epoch := Nat.not_le.mpr hact
        simp [h1, h2]
      · cases hf; rfl

/-- `process_registry_updates` keeps the `activeKey` of each validator at the current epoch,
and so the total active balance. -/
theorem process_registry_updates_activeKey (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (hrows : RowsOk s) (current_epoch activation_epoch : Epoch)
    (hcurrent : get_current_epoch p s = .ok current_epoch)
    (hactivation : compute_activation_exit_epoch p current_epoch = .ok activation_epoch)
    (hcur : current_epoch < FAR_FUTURE_EPOCH)
    (h : process_registry_updates p total_active_balance s = .ok s') :
    s'.validators.map (activeKey current_epoch) = s.validators.map (activeKey current_epoch) := by
  have hact : current_epoch < activation_epoch := by
    rw [compute_activation_exit_epoch_ok p current_epoch activation_epoch hactivation]
    exact Nat.lt_of_lt_of_le (Nat.lt_succ_self _) (Nat.le_add_right _ _)
  have hs := (process_registry_updates_rows p total_active_balance s current_epoch
    activation_epoch hrows hcurrent hactivation s').mp h
  cases hp : passM (registryRowStep p total_active_balance current_epoch
      s.finalized_checkpoint.epoch activation_epoch)
      (s.earliest_exit_epoch, s.exit_balance_to_consume) (rowsOf s) with
  | error e => rw [hp] at hs; cases hs
  | ok y =>
    rw [hp] at hs
    simp only [Functor.map, Except.map, Except.ok.injEq] at hs
    subst hs
    have hk := passM_keys _ (fun r => activeKey current_epoch r.validator)
      (fun a r x hx => registryRowStep_activeKey p total_active_balance current_epoch _
        activation_epoch hact hcur a r x hx) (rowsOf s) _ y hp
    simp only [BeaconState.withRows]
    rw [← rowsOf_map_validator s, List.map_map, List.map_map]
    exact hk

/-! ## A supply bound in place of the participation bound -/

/-- `lhRowStep_eq_singlePassStep_of_bounds` with a supply bound in place of `hincrements`.

`hincrements` asked that each flag has at most 256 times the active increments. Here each flag
has at most `k` times the active increments, for any `k ≤ 2^60 / 10^9`. A total effective
balance below `2^60` Gwei gives such a `k`. In exchange the row has a balance below `2^62` and
an effective balance at most `MAX_EFFECTIVE_BALANCE_ELECTRA`. -/
theorem lhRowStep_eq_singlePassStep_of_supply (ctx : LhStepContext) (rctx : RewardsContext)
    (r : Row) (churn : Epoch × Gwei) (base_reward : Gwei) (activation_epoch : Epoch)
    (hprev : rctx.previous_epoch = ctx.previous_epoch)
    (hleak : rctx.in_leak = ctx.in_leak)
    (hsource : rctx.source_increments = ctx.source_increments)
    (htarget : rctx.target_increments = ctx.target_increments)
    (hhead : rctx.head_increments = ctx.head_increments)
    (hactive : rctx.active_increments = ctx.active_increments)
    (hbase : rewardsEligible ctx.previous_epoch r.validator = .ok true →
      rewardsBaseReward Preset.mainnet rctx.total_active_balance r.validator = .ok base_reward)
    (htotal : Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT ≤ rctx.total_active_balance)
    (k : Nat) (hk : k ≤ 1152921504)
    (hincrements : ctx.source_increments ≤ k * ctx.active_increments ∧
      ctx.target_increments ≤ k * ctx.active_increments ∧
      ctx.head_increments ≤ k * ctx.active_increments)
    (heffective : rewardsEligible ctx.previous_epoch r.validator = .ok true →
      r.validator.effective_balance ≤ 256 * r.balance)
    (hmax : r.validator.effective_balance ≤ Preset.mainnet.MAX_EFFECTIVE_BALANCE_ELECTRA)
    (hbalance : r.balance < 2 ^ 62)
    (hfinalized : ctx.finalized_epoch ≤ ctx.current_epoch)
    (hcurrent : ctx.current_epoch + 1 < UINT64_SIZE)
    (hactivation : compute_activation_exit_epoch Preset.mainnet ctx.current_epoch =
      .ok activation_epoch)
    (hexit : r.validator.exit_epoch ≤ FAR_FUTURE_EPOCH) :
    lhRowStep Preset.mainnet ctx base_reward churn r =
      (fun x => (x.1.1.2, x.2)) <$> singlePassStep Preset.mainnet ctx.total_active_balance
        ctx.previous_epoch ctx.in_leak rctx ctx.current_epoch ctx.finalized_epoch
        activation_epoch ctx.slashings_target ctx.penalty_per_increment ((((), ()), churn), ())
        r := by
  refine lhRowStep_eq_singlePassStep_flags Preset.mainnet ctx rctx r churn base_reward
    activation_epoch hprev hleak hsource htarget hhead hactive hbase ?_ (by decide)
    hfinalized hcurrent hactivation hexit
  intro r' deltas hi hd
  obtain ⟨hv, -⟩ := inactivityRowStep_keeps _ _ _ _ _ hi
  have hmax' : @LE.le Nat _ r.validator.effective_balance 2048000000000 := hmax
  cases hel : rewardsEligible ctx.previous_epoch r.validator with
  | error e =>
    simp [rewardsRowDeltas, hv, hprev, hel, bind, Except.bind] at hd
  | ok el =>
  cases el
  case false =>
    simp [rewardsRowDeltas, hv, hprev, hel, bind, Except.bind, pure, Except.pure] at hd
    subst hd
    simp only [List.take, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
    have hb : r.balance < 2 ^ 64 := Nat.lt_trans hbalance (by decide)
    exact ⟨Nat.zero_le _, by simpa using hb, by decide⟩
  have heffective := heffective hel
  obtain ⟨s, t, h, i, base, hds, hb, hpen, hrew, hinact⟩ :=
    rewardsRowDeltas_bounds _ _ _ _ hd
  rw [hv] at hb
  have hbase256 : 256 * base ≤ r.validator.effective_balance := by
    rcases hb with hb | hb
    · subst hb; exact Nat.zero_le _
    · exact rewardsBaseReward_bound_mainnet _ _ _ htotal hb
  have hr := hrew k (hsource ▸ hactive ▸ hincrements.1) (htarget ▸ hactive ▸ hincrements.2.1)
    (hhead ▸ hactive ▸ hincrements.2.2)
  have hq : Preset.mainnet.INACTIVITY_SCORE_BIAS *
      Preset.mainnet.INACTIVITY_PENALTY_QUOTIENT_BELLATRIX = 67108864 := rfl
  rw [hq] at hinact
  subst hds
  obtain ⟨(b : Nat), hbal⟩ : ∃ b : Nat, r.balance = b := ⟨_, rfl⟩
  obtain ⟨(e : Nat), heb⟩ : ∃ e : Nat, r.validator.effective_balance = e := ⟨_, rfl⟩
  rw [hbal] at heffective hbalance ⊢
  rw [heb] at heffective hbase256 hmax'
  have he1 : e ≤ 256 * b := heffective
  have hb62 : @LT.lt Nat _ b (2 ^ 62) := hbalance
  have hb8 : base ≤ 8000000000 := by omega
  have hkb : k * base ≤ 1152921504 * 8000000000 := Nat.mul_le_mul hk hb8
  simp only [List.take, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  show s.2 + (t.2 + (h.2 + 0)) ≤ b ∧ b + (s.1 + (t.1 + (h.1 + (0 + 0)))) < 2 ^ 64 ∧
    s.2 + (t.2 + (h.2 + (i + 0))) < 2 ^ 64
  omega

/-! ## Participation sums are below the supply -/

/-- The sum of all effective balances. -/
def totalEffectiveBalance (s : BeaconState) : Gwei :=
  (s.validators.map (·.effective_balance)).sum

/-- A filtered sum is at most the full sum. -/
private theorem sum_filter_map_le {α : Type} (P : α → Bool) (f : α → Nat) :
    ∀ l : List α, ((l.filter P).map f).sum ≤ (l.map f).sum
  | [] => Nat.le_refl _
  | a :: l => by
    have ih := sum_filter_map_le P f l
    by_cases hp : P a = true
    · simp [hp]; omega
    · simp [hp]; omega

/-- The sum of the effective balances of some rows is at most the supply. -/
private theorem rows_sum_le (s : BeaconState) (Q : Row → Bool) :
    (((rowsOf s).filter Q).map (·.validator.effective_balance)).sum ≤ totalEffectiveBalance s := by
  have h := sum_filter_map_le Q (·.validator.effective_balance) (rowsOf s)
  have hm : (rowsOf s).map (·.validator.effective_balance) =
      s.validators.map (·.effective_balance) := by
    rw [← rowsOf_map_validator s, List.map_map]
    rfl
  rw [hm] at h
  exact h

/-- Reading the effective balances at the indices of some rows of the state. -/
private theorem mapM_rows_effective_balance (s : BeaconState) :
    ∀ rows : List Row, (∀ r ∈ rows, r ∈ rowsOf s) →
      (rows.map (·.index)).mapM (fun index => do
          pure (← listGet s.validators index).effective_balance) =
        (.ok (rows.map (·.validator.effective_balance)) : SpecM (List Gwei))
  | [], _ => rfl
  | r :: rows, h => by
    have ih := mapM_rows_effective_balance s rows (fun x hx => h x (by simp [hx]))
    simp only [List.map_cons, List.mapM_cons, listGet_rows_validator (h r (by simp)), ih]
    rfl

/-- `get_total_balance` of the indices of some rows: the sum, at least one increment. -/
theorem get_total_balance_rows (p : Preset) (s : BeaconState) (Q : Row → Bool)
    (hsupply : totalEffectiveBalance s < 2 ^ 64) :
    get_total_balance p s (((rowsOf s).filter Q).map (·.index)) =
      .ok (max p.EFFECTIVE_BALANCE_INCREMENT
        (((rowsOf s).filter Q).map (·.validator.effective_balance)).sum) := by
  unfold get_total_balance
  rw [mapM_rows_effective_balance s _ (fun r hr => (List.mem_filter.mp hr).1)]
  have hle := rows_sum_le s Q
  have hlt : (((rowsOf s).filter Q).map (·.validator.effective_balance)).sum < UINT64_SIZE :=
    Nat.lt_of_le_of_lt hle hsupply
  simp [uint64Sum, hlt, bind, Except.bind, pure, Except.pure]

/-- `get_total_active_balance` succeeds below the supply bound, is at least one increment and
at most the supply. -/
theorem get_total_active_balance_bounds (p : Preset) (s : BeaconState) (epoch : Epoch)
    (hepoch : get_current_epoch p s = .ok epoch)
    (hsupply : totalEffectiveBalance s < 2 ^ 64) :
    ∃ t, get_total_active_balance p s = .ok t ∧ p.EFFECTIVE_BALANCE_INCREMENT ≤ t ∧
      t ≤ max p.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s) := by
  unfold get_total_active_balance
  rw [hepoch]
  show ∃ t, get_total_balance p s (get_active_validator_indices s epoch) = .ok t ∧ _
  rw [get_active_validator_indices_rows, get_total_balance_rows p s _ hsupply]
  exact ⟨_, rfl, Nat.le_max_left _ _, Nat.max_le_of_le_of_le (Nat.le_max_left _ _)
    (Nat.le_trans (rows_sum_le s _) (Nat.le_max_right _ _))⟩

/-! ## The rewards context below the supply bound

The next lemmas are copies of private lemmas of `RowsRewards.lean`. -/

/-- A filtering loop where each test succeeds is `List.filter`. -/
private theorem forIn_filter_ok {X Y : Type} (P : X → SpecM Bool) (Q : X → Bool) (out : X → Y)
    (body : X → List Y → SpecM (ForInStep (List Y)))
    (hbody : ∀ x acc, body x acc =
      (fun b => ForInStep.yield (if b then acc ++ [out x] else acc)) <$> P x) :
    ∀ (l : List X) (acc : List Y), (∀ x ∈ l, P x = .ok (Q x)) →
      forIn l acc body = .ok (acc ++ (l.filter Q).map out)
  | [], acc, _ => by simp [pure, Except.pure]
  | x :: l, acc, h => by
    rw [List.forIn_cons, hbody, h x (by simp)]
    simp only [Functor.map, Except.map, bind, Except.bind]
    rw [forIn_filter_ok P Q out body hbody l _ (fun y hy => h y (by simp [hy]))]
    cases hq : Q x <;> simp [hq]

/-- The row at position `i` has index `i`. -/
private theorem rowsOf_index_getElem (state : BeaconState) (i : Nat)
    (h : i < (rowsOf state).length) : (rowsOf state)[i].index = i := by
  simp [rowsOf_getElem]

/-- A row of the state, as a position. -/
private theorem mem_rowsOf_iff {state : BeaconState} {r : Row} (h : r ∈ rowsOf state) :
    ∃ i, ∃ hi : i < (rowsOf state).length, (rowsOf state)[i] = r :=
  List.getElem_of_mem h

/-- The index of a row is a valid validator index. -/
private theorem rowsOf_index_lt {state : BeaconState} {r : Row} (h : r ∈ rowsOf state) :
    r.index < state.validators.length := by
  obtain ⟨i, hi, rfl⟩ := mem_rowsOf_iff h
  rw [rowsOf_index_getElem]
  simpa [rowsOf_length] using hi

/-- A list with one entry per validator has an entry at the index of each row. -/
private theorem getD_row {α : Type} (l : List α) (d : α) (n : Nat) (hl : l.length = n)
    {state : BeaconState} (hn : state.validators.length = n) {r : Row} (h : r ∈ rowsOf state) :
    listGet l r.index = .ok (l.getD r.index d) := by
  have := rowsOf_index_lt h
  have hlt : r.index < l.length := by omega
  simp [listGet, hlt, List.getD_eq_getElem?_getD, pure, Except.pure]

/-- The previous participation at the index of a row is the field of the row. -/
private theorem previous_participation_row {state : BeaconState} {r : Row} (h : r ∈ rowsOf state) :
    state.previous_epoch_participation.getD r.index 0 = r.previous_participation := by
  obtain ⟨i, hi, rfl⟩ := mem_rowsOf_iff h
  simp [rowsOf_getElem]

/-- `has_flag` is `rewardsHasFlag` for a flag index below 8. -/
private theorem has_flag_eq (flags : ParticipationFlags) (flag_index : Nat)
    (h : 2 ^ flag_index < 256) :
    has_flag flags flag_index = .ok (rewardsHasFlag flags flag_index) := by
  simp [has_flag, rewardsHasFlag, h, pure, Except.pure]

/-- `get_unslashed_participating_indices` for the previous epoch is the indices of the
participating rows. -/
private theorem participating_ok (p : Preset) (state : BeaconState) (hrows : RowsOk state)
    (current_epoch previous_epoch : Epoch) (flag_index : Nat)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (hprevious : get_previous_epoch p state = .ok previous_epoch)
    (hne : previous_epoch ≠ current_epoch) (hflag : 2 ^ flag_index < 256) :
    get_unslashed_participating_indices p state flag_index previous_epoch =
      .ok (((rowsOf state).filter fun r => rewardsParticipating previous_epoch r flag_index).map
        (·.index)) := by
  have hne' : (previous_epoch == current_epoch) = false := by simpa using hne
  unfold get_unslashed_participating_indices
  simp only [bind, Except.bind, hprevious, hcurrent, BEq.rfl, Bool.true_or, Bool.not_true,
    Bool.false_eq_true, if_false, pure, Except.pure, hne']
  rw [get_active_validator_indices_rows]
  rw [forIn_filter_ok
    (fun i => do has_flag (← listGet state.previous_epoch_participation i) flag_index)
    (fun i => rewardsHasFlag (state.previous_epoch_participation.getD i 0) flag_index) id]
  rotate_left
  · intro i acc
    simp only [bind, Except.bind]
    cases listGet state.previous_epoch_participation i with
    | error e => rfl
    | ok f =>
      cases hf : has_flag f flag_index with
      | error e => simp [hf, Functor.map, Except.map]
      | ok b => cases b <;> simp [hf, Functor.map, Except.map]
  · intro i hi
    obtain ⟨r, hr, rfl⟩ := List.mem_map.mp hi
    have hr' := (List.mem_filter.mp hr).1
    rw [getD_row _ 0 state.validators.length hrows.2.2.1 rfl hr']
    exact has_flag_eq _ _ hflag
  dsimp only
  rw [forIn_filter_ok (fun i => (fun v => !v.slashed) <$> listGet state.validators i)
    (fun i => ((state.validators[i]?).map (!·.slashed)).getD false) id]
  rotate_left
  · intro i acc
    cases listGet state.validators i with
    | error e => rfl
    | ok v => cases hs : v.slashed <;> simp [hs, Functor.map, Except.map]
  · intro i hi
    simp only [List.nil_append, List.map_id_fun, id_eq, List.mem_filter, List.mem_map] at hi
    obtain ⟨⟨r, hr, rfl⟩, -⟩ := hi
    have hlt := rowsOf_index_lt hr.1
    simp [listGet, hlt, pure, Except.pure, Functor.map, Except.map]
  simp only [List.nil_append, List.map_id_fun, id_eq, List.filter_map, List.filter_filter,
    Function.comp_def]
  congr 2
  apply List.filter_congr
  intro r hr
  have hlt := rowsOf_index_lt hr
  rw [previous_participation_row hr]
  obtain ⟨i, hi, rfl⟩ := mem_rowsOf_iff hr
  simp only [rewardsParticipating, rowsOf_getElem] at hlt ⊢
  simp only [hlt, getElem?_pos, Option.map_some, Option.getD_some]
  cases is_active_validator state.validators[i] previous_epoch <;>
    cases rewardsHasFlag (state.previous_epoch_participation.getD i 0) flag_index <;>
    cases state.validators[i].slashed <;> rfl

/-- One flag has at most the supply in increments. -/
theorem rewardsFlagIncrements_bound (p : Preset) (s : BeaconState) (hrows : RowsOk s)
    (current_epoch previous_epoch : Epoch) (flag_index : Nat)
    (hcurrent : get_current_epoch p s = .ok current_epoch)
    (hprevious : get_previous_epoch p s = .ok previous_epoch)
    (hne : previous_epoch ≠ current_epoch) (hflag : 2 ^ flag_index < 256)
    (hinc : p.EFFECTIVE_BALANCE_INCREMENT ≠ 0)
    (hsupply : totalEffectiveBalance s < 2 ^ 64) :
    ∃ inc, rewardsFlagIncrements p s previous_epoch flag_index = .ok inc ∧
      inc ≤ max p.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s) /
        p.EFFECTIVE_BALANCE_INCREMENT := by
  unfold rewardsFlagIncrements
  rw [participating_ok p s hrows current_epoch previous_epoch flag_index hcurrent hprevious hne
    hflag]
  have hgt := get_total_balance_rows p s (fun r => rewardsParticipating previous_epoch r flag_index)
    hsupply
  simp only [bind, Except.bind, hgt, uint64Div, hinc, if_false, pure, Except.pure,
    Except.ok.injEq, exists_eq_left']
  exact Nat.div_le_div_right (Nat.max_le_of_le_of_le (Nat.le_max_left _ _)
    (Nat.le_trans (rows_sum_le s _) (Nat.le_max_right _ _)))

/-- The rewards context succeeds below the supply bound, if the finalized epoch is not after the
previous epoch. Each flag then has at most the supply in increments. -/
theorem rewardsContextOf_bounds (p : Preset) (total_active_balance : Gwei) (s : BeaconState)
    (hrows : RowsOk s) (current_epoch previous_epoch : Epoch)
    (hcurrent : get_current_epoch p s = .ok current_epoch)
    (hprevious : get_previous_epoch p s = .ok previous_epoch)
    (hne : previous_epoch ≠ current_epoch) (hinc : p.EFFECTIVE_BALANCE_INCREMENT ≠ 0)
    (hsupply : totalEffectiveBalance s < 2 ^ 64)
    (hfinalized : s.finalized_checkpoint.epoch ≤ previous_epoch) :
    ∃ rctx, rewardsContextOf p total_active_balance s = .ok rctx ∧
      rctx.source_increments ≤ max p.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s) /
        p.EFFECTIVE_BALANCE_INCREMENT ∧
      rctx.target_increments ≤ max p.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s) /
        p.EFFECTIVE_BALANCE_INCREMENT ∧
      rctx.head_increments ≤ max p.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s) /
        p.EFFECTIVE_BALANCE_INCREMENT := by
  obtain ⟨si, hs, hsb⟩ := rewardsFlagIncrements_bound p s hrows current_epoch previous_epoch
    TIMELY_SOURCE_FLAG_INDEX hcurrent hprevious hne (by decide) hinc hsupply
  obtain ⟨ti, ht, htb⟩ := rewardsFlagIncrements_bound p s hrows current_epoch previous_epoch
    TIMELY_TARGET_FLAG_INDEX hcurrent hprevious hne (by decide) hinc hsupply
  obtain ⟨hi, hh, hhb⟩ := rewardsFlagIncrements_bound p s hrows current_epoch previous_epoch
    TIMELY_HEAD_FLAG_INDEX hcurrent hprevious hne (by decide) hinc hsupply
  have hleak : ∃ b, is_in_inactivity_leak p s = .ok b := by
    simp [is_in_inactivity_leak, get_finality_delay, hprevious, uint64Sub, hfinalized, bind,
      Except.bind, pure, Except.pure]
  obtain ⟨b, hb⟩ := hleak
  refine ⟨⟨previous_epoch, b, total_active_balance, si, ti, hi,
    total_active_balance / p.EFFECTIVE_BALANCE_INCREMENT⟩, ?_, hsb, htb, hhb⟩
  simp [rewardsContextOf, hprevious, hs, ht, hh, hb, uint64Div, hinc, bind, Except.bind, pure,
    Except.pure]

/-! ## The single pass with the supply bound -/

/-- `lighthouse_single_pass_eq_spec` with the supply facts in place of `hincrements` and of the
supply part of `EpochEntry`: each flag has at most `k` times the active increments for some
`k ≤ 2^60 / 10^9`, each balance is below `2^62`, and each effective balance is at most
`MAX_EFFECTIVE_BALANCE_ELECTRA`. -/
theorem lighthouse_single_pass_eq_spec_supply (total_active_balance : Gwei) (state : BeaconState)
    (hrows : RowsOk state) (current_epoch : Epoch)
    (hcurrent : get_current_epoch Preset.mainnet state = .ok current_epoch)
    (hgenesis : current_epoch ≠ GENESIS_EPOCH)
    (rctx : RewardsContext)
    (hctx : rewardsContextOf Preset.mainnet total_active_balance state = .ok rctx)
    (heffective : ∀ i (h : i < state.validators.length),
      rewardsEligible rctx.previous_epoch state.validators[i] = .ok true →
        state.validators[i].effective_balance ≤ 256 * state.balances.getD i 0)
    (hexit : ∀ i (h : i < state.validators.length),
      state.validators[i].exit_epoch ≤ FAR_FUTURE_EPOCH)
    (hmax : ∀ v ∈ state.validators,
      v.effective_balance ≤ Preset.mainnet.MAX_EFFECTIVE_BALANCE_ELECTRA)
    (hbalance : ∀ i (h : i < state.validators.length), state.balances.getD i 0 < 2 ^ 62)
    (activation_epoch : Epoch)
    (hactivation : compute_activation_exit_epoch Preset.mainnet current_epoch =
      .ok activation_epoch)
    (htarget : current_epoch + Preset.mainnet.EPOCHS_PER_SLASHINGS_VECTOR / 2 < UINT64_SIZE)
    (adjusted per : Gwei)
    (hpre : slashingsPreamble Preset.mainnet total_active_balance state.slashings =
      .ok (adjusted, per))
    (hfinalized : state.finalized_checkpoint.epoch ≤ current_epoch)
    (hcurrent64 : current_epoch + 1 < UINT64_SIZE)
    (htotal : Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT ≤ total_active_balance)
    (k : Nat) (hk : k ≤ 1152921504)
    (hincrements : rctx.source_increments ≤ k * rctx.active_increments ∧
      rctx.target_increments ≤ k * rctx.active_increments ∧
      rctx.head_increments ≤ k * rctx.active_increments)
    (base_reward : Row → Gwei)
    (hbase : ∀ r ∈ rowsOf state, rewardsEligible rctx.previous_epoch r.validator = .ok true →
      rewardsBaseReward Preset.mainnet total_active_balance r.validator = .ok (base_reward r)) :
    SameOk (do
      let s1 ← process_inactivity_updates Preset.mainnet state
      let s2 ← process_rewards_and_penalties Preset.mainnet total_active_balance s1
      let s3 ← process_registry_updates Preset.mainnet total_active_balance s2
      process_slashings Preset.mainnet total_active_balance s3)
    ((fun y => state.withRowsChurn y.1 y.2) <$>
      passM (fun churn r => lhRowStep Preset.mainnet
          (lhContextOf state total_active_balance current_epoch rctx per) (base_reward r) churn r)
        (state.earliest_exit_epoch, state.exit_balance_to_consume) (rowsOf state)) := by
  obtain ⟨hprevious, -, -, -, -, hleak, htab⟩ :=
    rewardsContextOf_ok Preset.mainnet total_active_balance state rctx hctx
  refine SameOk.trans (separate_passes_eq_single_pass Preset.mainnet total_active_balance state
    hrows current_epoch rctx.previous_epoch rctx.in_leak hcurrent hgenesis hprevious hleak rctx
    hctx activation_epoch hactivation htarget adjusted per hpre) (SameOk.of_eq ?_)
  have hstep : ∀ churn r, r ∈ rowsOf state →
      lhRowStep Preset.mainnet (lhContextOf state total_active_balance current_epoch rctx per)
        (base_reward r) churn r =
      (fun x => (x.1.1.2, x.2)) <$> singlePassStep Preset.mainnet total_active_balance
        rctx.previous_epoch rctx.in_leak rctx current_epoch state.finalized_checkpoint.epoch
        activation_epoch (current_epoch + Preset.mainnet.EPOCHS_PER_SLASHINGS_VECTOR / 2) per
        ((((), ()), churn), ()) r := by
    intro churn r hr
    obtain ⟨i, hi, hri⟩ := List.mem_iff_getElem.mp hr
    have hiv : i < state.validators.length := by rw [← rowsOf_length]; exact hi
    have hrow := rowsOf_getElem state i hi
    rw [hri] at hrow
    subst hrow
    exact lhRowStep_eq_singlePassStep_of_supply _ rctx _ churn (base_reward _) activation_epoch
      rfl rfl rfl rfl rfl rfl
      (by rw [htab]; exact hbase _ hr) (by rw [htab]; exact htotal) k hk hincrements
      (heffective i hiv) (hmax _ (List.getElem_mem hiv)) (hbalance i hiv) hfinalized hcurrent64
      hactivation (hexit i hiv)
  rw [passM_congr _ _ (rowsOf state) (fun churn r hr => hstep churn r hr)]
  rw [passM_proj (singlePassStep Preset.mainnet total_active_balance rctx.previous_epoch
    rctx.in_leak rctx current_epoch state.finalized_checkpoint.epoch activation_epoch
    (current_epoch + Preset.mainnet.EPOCHS_PER_SLASHINGS_VECTOR / 2) per)
    (fun c => ((((), ()), c), ())) (fun b => b.1.2) (fun _ => rfl) (fun _ => rfl)]
  cases passM _ _ (rowsOf state) <;> rfl

/-! ## `process_epoch_lh` -/

/-- The base reward of a row, as the epoch cache holds it. It is 0 when `get_base_reward`
fails, and the pass reads it only for eligible rows. -/
def lhBaseReward (total_active_balance : Gwei) (r : Row) : Gwei :=
  match rewardsBaseReward Preset.mainnet total_active_balance r.validator with
  | .ok b => b
  | .error _ => 0

/-- The four spec steps that the Lighthouse pass replaces, as `process_epoch` runs them. -/
def specFourSteps (p : Preset) (s : BeaconState) : SpecM BeaconState := do
  let s ← process_inactivity_updates p s
  let s ← process_rewards_and_penalties p (← get_total_active_balance p s) s
  let s ← process_registry_updates p (← get_total_active_balance p s) s
  process_slashings p (← get_total_active_balance p s) s

/-- The Lighthouse single pass on mainnet: the inactivity, rewards, registry and slashings
steps in one pass over the rows. The context comes from the state before the pass. -/
def lhPass (s : BeaconState) : SpecM BeaconState := do
  let current_epoch ← get_current_epoch Preset.mainnet s
  let total_active_balance ← get_total_active_balance Preset.mainnet s
  let rctx ← rewardsContextOf Preset.mainnet total_active_balance s
  let (_, per) ← slashingsPreamble Preset.mainnet total_active_balance s.slashings
  (fun y => s.withRowsChurn y.1 y.2) <$>
    passM (fun churn r => lhRowStep Preset.mainnet
        (lhContextOf s total_active_balance current_epoch rctx per)
        (lhBaseReward total_active_balance r) churn r)
      (s.earliest_exit_epoch, s.exit_balance_to_consume) (rowsOf s)

/-- The steps of `process_epoch` after `process_slashings`. -/
def epochRest (p : Preset) (o : Oracle) (state : BeaconState) : SpecM BeaconState := do
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

/-- `process_epoch` in three parts. -/
theorem process_epoch_parts (p : Preset) (o : Oracle) (s : BeaconState) :
    process_epoch p o s = (do
      let s1 ← process_justification_and_finalization p s
      let s5 ← specFourSteps p s1
      epochRest p o s5) := by
  simp only [process_epoch, specFourSteps, epochRest, bind_assoc]

/-- Justification and finalization, the Lighthouse single pass, then the rest of the epoch. -/
def process_epoch_lh_core (o : Oracle) (s : BeaconState) : SpecM BeaconState := do
  let s1 ← process_justification_and_finalization Preset.mainnet s
  let s5 ← lhPass s1
  epochRest Preset.mainnet o s5

/-- The Gloas `process_epoch` on mainnet, with the Lighthouse single pass in place of the
inactivity, rewards, registry and slashings steps. At the genesis epoch the spec skips the
inactivity and rewards steps. Lighthouse also skips them there, through `after_genesis`, and
this one epoch uses the spec path. -/
def process_epoch_lh (o : Oracle) (s : BeaconState) : SpecM BeaconState :=
  if s.slot / Preset.mainnet.SLOTS_PER_EPOCH = GENESIS_EPOCH then process_epoch Preset.mainnet o s
  else process_epoch_lh_core o s

/-! ## The state after justification and finalization -/

/-- `t` is `s` with new checkpoints and justification bits. -/
def JFShape (s t : BeaconState) : Prop :=
  ∃ (a b c : Checkpoint) (d : List Bool), t = { s with
    previous_justified_checkpoint := a
    current_justified_checkpoint := b
    finalized_checkpoint := c
    justification_bits := d }

/-- `JFShape` is transitive. -/
theorem JFShape.trans {s t u : BeaconState} (h1 : JFShape s t) (h2 : JFShape t u) :
    JFShape s u := by
  obtain ⟨_, _, _, _, rfl⟩ := h1
  obtain ⟨_, _, _, _, rfl⟩ := h2
  exact ⟨_, _, _, _, rfl⟩

/-- Splits one step of `weigh_justification_and_finalization` and keeps `JFShape`. -/
local macro "jf_step" ht:ident q:ident : tactic => `(tactic| (
  simp only [bind, Except.bind, pure, Except.pure] at $ht:ident
  repeat' split at $ht:ident
  all_goals first
    | contradiction
    | (cases $ht:ident; exact JFShape.trans $q:ident ⟨_, _, _, _, rfl⟩)
    | (cases $ht:ident; exact $q:ident)))

/-- `weigh_justification_and_finalization` writes only the checkpoints and the bits. -/
theorem weigh_justification_and_finalization_shape (p : Preset) (s s' : BeaconState)
    (total_active_balance previous_epoch_target_balance current_epoch_target_balance : Gwei)
    (h : weigh_justification_and_finalization p s total_active_balance
      previous_epoch_target_balance current_epoch_target_balance = .ok s') : JFShape s s' := by
  unfold weigh_justification_and_finalization at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨b0, -, h⟩ := specM_bind_ok h
  have q0 : JFShape s { s with
      previous_justified_checkpoint := s.current_justified_checkpoint,
      justification_bits := b0 } := ⟨_, _, _, _, rfl⟩
  obtain ⟨t1, ht1, h⟩ := specM_bind_ok h
  have q1 : JFShape s t1 := by jf_step ht1 q0
  obtain ⟨t2, ht2, h⟩ := specM_bind_ok h
  have q2 : JFShape s t2 := by jf_step ht2 q1
  obtain ⟨t3, ht3, h⟩ := specM_bind_ok h
  have q3 : JFShape s t3 := by jf_step ht3 q2
  obtain ⟨t4, ht4, h⟩ := specM_bind_ok h
  have q4 : JFShape s t4 := by jf_step ht4 q3
  obtain ⟨t5, ht5, h⟩ := specM_bind_ok h
  have q5 : JFShape s t5 := by jf_step ht5 q4
  jf_step h q5

/-- `process_justification_and_finalization` writes only the checkpoints and the bits. -/
theorem process_justification_and_finalization_shape (p : Preset) (s s' : BeaconState)
    (h : process_justification_and_finalization p s = .ok s') : JFShape s s' := by
  unfold process_justification_and_finalization at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact ⟨_, _, _, _, rfl⟩)
    | exact weigh_justification_and_finalization_shape p _ _ _ _ _ h

/-- A successful `get_current_epoch` is the slot divided by the epoch length. -/
private theorem current_epoch_eq {p : Preset} {s : BeaconState} {e : Epoch}
    (h : get_current_epoch p s = .ok e) : e = s.slot / p.SLOTS_PER_EPOCH := by
  simp only [get_current_epoch, compute_epoch_at_slot, uint64Div] at h
  split at h
  · cases h
  · cases h; rfl

/-- Under `FfgInvariant`, after `process_justification_and_finalization` the finalized epoch is
before the current epoch. -/
theorem process_justification_and_finalization_finalized (p : Preset) (s s' : BeaconState)
    (current_epoch : Epoch) (hcurrent : get_current_epoch p s = .ok current_epoch)
    (hgenesis : current_epoch ≠ GENESIS_EPOCH) (hinv : FfgInvariant p s)
    (h : process_justification_and_finalization p s = .ok s') :
    s'.finalized_checkpoint.epoch + 1 ≤ current_epoch := by
  have hc := current_epoch_eq hcurrent
  obtain ⟨⟨hfp, hpc⟩, hcj⟩ := hinv
  simp only [epochOf] at hcj
  simp only [GENESIS_EPOCH] at hgenesis
  rcases process_justification_and_finalization_step p s s' h with rfl | ⟨-, hw⟩
  · rcases hcj with hcj | hcj <;> epoch_omega
  · obtain ⟨-, -, -, -, hfin⟩ := hw
    rcases hfin with h1 | h1 | h1 <;> rw [h1] <;> rcases hcj with hcj | hcj <;> epoch_omega

/-! ## One total active balance for the four steps -/

/-- An `ok` value followed by more code. -/
private theorem ok_bind {α β : Type} (a : α) (f : α → SpecM β) : (Except.ok a >>= f) = f a := rfl

/-- The current epoch reads only the slot. -/
private theorem current_epoch_of_slot {p : Preset} {s s' : BeaconState} {e : Epoch}
    (h : get_current_epoch p s = .ok e) (hslot : s'.slot = s.slot) :
    get_current_epoch p s' = .ok e := by
  rw [← h]; simp only [get_current_epoch, hslot]

/-- Inside `process_epoch`, the total active balance does not change during the inactivity,
rewards and registry steps. So the four steps use the total of the state before them. -/
theorem specFourSteps_sameOk (p : Preset) (s1 : BeaconState) (total_active_balance : Gwei)
    (htab : get_total_active_balance p s1 = .ok total_active_balance) (hrows : RowsOk s1)
    (current_epoch activation_epoch : Epoch)
    (hcurrent : get_current_epoch p s1 = .ok current_epoch)
    (hactivation : compute_activation_exit_epoch p current_epoch = .ok activation_epoch)
    (hcur : current_epoch < FAR_FUTURE_EPOCH) :
    SameOk (specFourSteps p s1) (do
      let s2 ← process_inactivity_updates p s1
      let s3 ← process_rewards_and_penalties p total_active_balance s2
      let s4 ← process_registry_updates p total_active_balance s3
      process_slashings p total_active_balance s4) := by
  unfold specFourSteps
  refine SameOk.bind (SameOk.refl _) (fun s2 h2 => ?_)
  have hv2 := process_inactivity_updates_validators p _ _ h2
  have hslot2 := (process_inactivity_updates_checkpointsStable p _ _ h2).1
  have hl2 := process_inactivity_updates_len p _ _ h2
  have t2 : get_total_active_balance p s2 = .ok total_active_balance := by
    rw [get_total_active_balance_congr_key p s1 s2 current_epoch hcurrent hslot2 (by rw [hv2])]
    exact htab
  rw [t2, ok_bind]
  refine SameOk.bind (SameOk.refl _) (fun s3 h3 => ?_)
  have hv3 := process_rewards_and_penalties_validators p _ _ _ h3
  have hslot3 := (process_rewards_and_penalties_checkpointsStable p _ _ _ h3).1
  have hl3 := process_rewards_and_penalties_len p _ _ _ h3
  have hc3 : get_current_epoch p s3 = .ok current_epoch :=
    current_epoch_of_slot hcurrent (hslot3.trans hslot2)
  have t3 : get_total_active_balance p s3 = .ok total_active_balance := by
    rw [get_total_active_balance_congr_key p s2 s3 current_epoch
      (current_epoch_of_slot hcurrent hslot2) hslot3 (by rw [hv3])]
    exact t2
  rw [t3, ok_bind]
  refine SameOk.bind (SameOk.refl _) (fun s4 h4 => ?_)
  have hslot4 := (process_registry_updates_checkpointsStable p _ _ _ h4).1
  have hrows3 : RowsOk s3 := (hl2.trans hl3).rowsOk hrows
  have hkey := process_registry_updates_activeKey p _ s3 s4 hrows3 current_epoch
    activation_epoch hc3 hactivation hcur h4
  have t4 : get_total_active_balance p s4 = .ok total_active_balance := by
    rw [get_total_active_balance_congr_key p s3 s4 current_epoch hc3 hslot4 hkey]
    exact t3
  rw [t4, ok_bind]
  exact SameOk.refl _

/-! ## The context computations succeed -/

/-- `compute_activation_exit_epoch` succeeds when the epoch is far below `2^64`. -/
theorem compute_activation_exit_epoch_of_lt (p : Preset) (epoch : Epoch)
    (h : epoch + 1 + p.MAX_SEED_LOOKAHEAD < UINT64_SIZE) :
    compute_activation_exit_epoch p epoch = .ok (epoch + 1 + p.MAX_SEED_LOOKAHEAD) := by
  have h1 : epoch + 1 < UINT64_SIZE := Nat.lt_of_le_of_lt (Nat.le_add_right _ _) h
  simp [compute_activation_exit_epoch, uint64Add, h1, h, bind, Except.bind, pure, Except.pure]

/-- On mainnet, `slashingsPreamble` succeeds if three times the slashed sum fits in a `u64`
and the total active balance is at least one increment. -/
theorem slashingsPreamble_ok (total_active_balance : Gwei) (slashings : List Gwei)
    (hmul : slashings.sum * 3 < UINT64_SIZE)
    (htotal : Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT ≤ total_active_balance) :
    ∃ adjusted per,
      slashingsPreamble Preset.mainnet total_active_balance slashings = .ok (adjusted, per) := by
  have hsum : slashings.sum < UINT64_SIZE :=
    Nat.lt_of_le_of_lt (Nat.le_mul_of_pos_right _ (by decide)) hmul
  have hinc : 0 < Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT := by decide
  have hq : total_active_balance / Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT ≠ 0 :=
    Nat.ne_of_gt (Nat.div_pos htotal hinc)
  have hm : Preset.mainnet.PROPORTIONAL_SLASHING_MULTIPLIER_BELLATRIX = 3 := rfl
  have hinc' : Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT ≠ 0 := by decide
  simp [slashingsPreamble, uint64Sum, hsum, uint64Mul, hm, hmul, uint64Div, hq, hinc', bind,
    Except.bind, pure, Except.pure]

/-- `saturating_sub e 1` is `e - 1`. -/
private theorem saturating_sub_one (e : Nat) : saturating_sub e 1 = e - 1 := by
  show (if e > 1 then e - 1 else e - e) = e - 1
  split <;> omega

/-- On mainnet, `get_base_reward` succeeds when the total is at least one increment and the
effective balance is at most `MAX_EFFECTIVE_BALANCE_ELECTRA`. -/
theorem rewardsBaseReward_ok_mainnet (total_active_balance : Gwei) (v : Validator)
    (htotal : Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT ≤ total_active_balance)
    (hmax : v.effective_balance ≤ Preset.mainnet.MAX_EFFECTIVE_BALANCE_ELECTRA) :
    ∃ b, rewardsBaseReward Preset.mainnet total_active_balance v = .ok b := by
  have ht : (1000000000 : Nat) ≤ total_active_balance := htotal
  have hroot : 31622 ≤ integer_squareroot total_active_balance :=
    le_integer_squareroot _ _ (Nat.le_trans (by decide) ht)
  obtain ⟨(r : Nat), hr⟩ : ∃ r : Nat, integer_squareroot total_active_balance = r := ⟨_, rfl⟩
  rw [hr] at hroot
  have hper : 1000000000 * 64 / r ≤ 2023907 :=
    Nat.le_trans (Nat.div_le_div_left hroot (by decide)) (by decide)
  have hmax' : @LE.le Nat _ v.effective_balance 2048000000000 := hmax
  have he : v.effective_balance / 1000000000 ≤ 2048 :=
    Nat.le_trans (Nat.div_le_div_right hmax') (by decide)
  have hprod : v.effective_balance / 1000000000 * (1000000000 * 64 / r) < UINT64_SIZE :=
    Nat.lt_of_le_of_lt (Nat.mul_le_mul he hper) (by decide)
  have hr0 : r ≠ 0 := Nat.ne_of_gt (Nat.lt_of_lt_of_le (by decide) hroot)
  refine ⟨v.effective_balance / 1000000000 * (1000000000 * 64 / r), ?_⟩
  have hm : (1000000000 : Nat) * 64 < UINT64_SIZE := by decide
  simp only [rewardsBaseReward, get_base_reward_per_increment, Preset.mainnet, uint64Div,
    uint64Mul, hr, hr0, hm, hprod, bind, Except.bind, pure, Except.pure, if_true, if_false,
    show (1000000000 : Nat) ≠ 0 from by decide]

/-! ## The whole epoch -/

/-- On mainnet, away from the genesis epoch, `process_epoch_lh_core` gives the same `ok`
results as `process_epoch`.

The hypotheses are facts about the state at the start of the epoch:
- `hrows`: the row lists have one entry per validator.
- `hinv`: `FfgInvariant`. After justification and finalization it puts the finalized epoch
  before the current epoch, so the inactivity leak test succeeds.
- `hgenesis`, `hepoch`: the current epoch is not the genesis epoch, and the slashings target
  epoch (current + 4096) fits in a `u64`.
- `heffective`, `hexit`: the effective balance and exit epoch parts of `EpochEntry`.
- `hmax`, `hbalance`, `hsupply`: each effective balance is at most
  `MAX_EFFECTIVE_BALANCE_ELECTRA`, each balance is below `2^62`, and the sum of the effective
  balances is below `2^60`. They replace the participation bound and the supply part of
  `EpochEntry`.
- `hslashings`: three times the sum of the slashings vector fits in a `u64`. -/
theorem process_epoch_lh_sameOk (o : Oracle) (s : BeaconState) (hrows : RowsOk s)
    (hinv : FfgInvariant Preset.mainnet s) (current_epoch : Epoch)
    (hcurrent : get_current_epoch Preset.mainnet s = .ok current_epoch)
    (hgenesis : current_epoch ≠ GENESIS_EPOCH)
    (hepoch : current_epoch + Preset.mainnet.EPOCHS_PER_SLASHINGS_VECTOR / 2 < UINT64_SIZE)
    (heffective : ∀ i (h : i < s.validators.length),
      rewardsEligible (current_epoch - 1) s.validators[i] = .ok true →
        s.validators[i].effective_balance ≤ 256 * s.balances.getD i 0)
    (hexit : ∀ i (h : i < s.validators.length), s.validators[i].exit_epoch ≤ FAR_FUTURE_EPOCH)
    (hmax : ∀ v ∈ s.validators,
      v.effective_balance ≤ Preset.mainnet.MAX_EFFECTIVE_BALANCE_ELECTRA)
    (hbalance : ∀ i (h : i < s.validators.length), s.balances.getD i 0 < 2 ^ 62)
    (hsupply : totalEffectiveBalance s < 2 ^ 60)
    (hslashings : s.slashings.sum * 3 < UINT64_SIZE) :
    SameOk (process_epoch Preset.mainnet o s) (process_epoch_lh_core o s) := by
  rw [process_epoch_parts]
  unfold process_epoch_lh_core
  refine SameOk.bind (SameOk.refl _) (fun s1 h1 => ?_)
  refine SameOk.bind ?_ (fun _ _ => SameOk.refl _)
  obtain ⟨_, _, _, _, hs1⟩ := process_justification_and_finalization_shape _ _ _ h1
  have hcur1 : get_current_epoch Preset.mainnet s1 = .ok current_epoch := by
    rw [hs1]; exact hcurrent
  have hfin := process_justification_and_finalization_finalized _ s s1 current_epoch hcurrent
    hgenesis hinv h1
  have hrows1 : RowsOk s1 := by rw [hs1]; exact hrows
  have heff1 : ∀ i (h : i < s1.validators.length),
      rewardsEligible (current_epoch - 1) s1.validators[i] = .ok true →
        s1.validators[i].effective_balance ≤ 256 * s1.balances.getD i 0 := by
    rw [hs1]; exact heffective
  have hexit1 : ∀ i (h : i < s1.validators.length),
      s1.validators[i].exit_epoch ≤ FAR_FUTURE_EPOCH := by rw [hs1]; exact hexit
  have hmax1 : ∀ v ∈ s1.validators,
      v.effective_balance ≤ Preset.mainnet.MAX_EFFECTIVE_BALANCE_ELECTRA := by
    rw [hs1]; exact hmax
  have hbal1 : ∀ i (h : i < s1.validators.length), s1.balances.getD i 0 < 2 ^ 62 := by
    rw [hs1]; exact hbalance
  have hS1 : totalEffectiveBalance s1 = totalEffectiveBalance s := by rw [hs1]; rfl
  have hsl1 : s1.slashings = s.slashings := by rw [hs1]
  rw [← hS1] at hsupply
  rw [← hsl1] at hslashings
  simp only [GENESIS_EPOCH] at hgenesis
  have h4096 : Preset.mainnet.EPOCHS_PER_SLASHINGS_VECTOR / 2 = 4096 := rfl
  rw [h4096] at hepoch
  have hS64 : totalEffectiveBalance s1 < 2 ^ 64 := Nat.lt_trans hsupply (by decide)
  obtain ⟨tab, htab, htab_lo, htab_hi⟩ :=
    get_total_active_balance_bounds Preset.mainnet s1 current_epoch hcur1 hS64
  have hprev : get_previous_epoch Preset.mainnet s1 = .ok (current_epoch - 1) := by
    have hsat : saturating_sub current_epoch 1 = current_epoch - 1 :=
      saturating_sub_one current_epoch
    simp only [get_previous_epoch, hcur1, ok_bind, hsat]
    rfl
  obtain ⟨rctx, hctx, hsrc, htgt, hhd⟩ := rewardsContextOf_bounds Preset.mainnet tab s1 hrows1
    current_epoch (current_epoch - 1) hcur1 hprev (by epoch_omega) (by decide) hS64 (by epoch_omega)
  obtain ⟨hprev', -, -, -, hactive, -, -⟩ := rewardsContextOf_ok _ _ _ rctx hctx
  rw [hprev] at hprev'
  have hpe : rctx.previous_epoch = current_epoch - 1 := (Except.ok.inj hprev').symm
  have hact_eq : rctx.active_increments = tab / Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT := by
    simp only [uint64Div] at hactive
    split at hactive
    · cases hactive
    · exact (Except.ok.inj hactive).symm
  have hactivation := compute_activation_exit_epoch_of_lt Preset.mainnet current_epoch
    (by simp only [Preset.mainnet]; epoch_omega)
  obtain ⟨adj, per, hpre⟩ := slashingsPreamble_ok tab s1.slashings hslashings htab_lo
  have hk : max Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s1) /
      Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT ≤ 1152921504 := by
    have : max Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s1) <
        2 ^ 60 := Nat.max_lt.mpr ⟨by decide, hsupply⟩
    have hINC : Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT = 1000000000 := rfl
    obtain ⟨(m : Nat), hm⟩ : ∃ m : Nat,
        max Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s1) = m :=
      ⟨_, rfl⟩
    rw [hm] at this ⊢
    rw [hINC]
    have h60 : m < 1152921504606846976 := this
    show m / 1000000000 ≤ 1152921504
    omega
  have hact1 : 1 ≤ rctx.active_increments := by
    rw [hact_eq]
    exact (Nat.le_div_iff_mul_le (by decide)).mpr (by simpa using htab_lo)
  have hle : ∀ x, x ≤ max Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s1) /
      Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT →
      x ≤ max Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s1) /
        Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT * rctx.active_increments :=
    fun x hx => Nat.le_trans hx (Nat.le_mul_of_pos_right _ hact1)
  have hcfar : current_epoch < FAR_FUTURE_EPOCH := by
    have h64 : @LT.lt Nat _ (current_epoch + 4096) (2 ^ 64) := hepoch
    show @LT.lt Nat _ current_epoch (2 ^ 64 - 1)
    omega
  refine SameOk.trans (specFourSteps_sameOk Preset.mainnet s1 tab htab hrows1 current_epoch _
    hcur1 hactivation hcfar) ?_
  refine SameOk.trans (lighthouse_single_pass_eq_spec_supply tab s1 hrows1 current_epoch hcur1
    (by simpa [GENESIS_EPOCH] using hgenesis) rctx hctx (hpe ▸ heff1) hexit1 hmax1 hbal1 _
    hactivation (by simp only [h4096]; exact hepoch) adj per hpre
    (Nat.le_of_succ_le hfin) (Nat.lt_of_le_of_lt (Nat.add_le_add_left (by decide) _) hepoch)
    htab_lo
    _ hk ⟨hle _ hsrc, hle _ htgt, hle _ hhd⟩ (lhBaseReward tab) ?_) (SameOk.of_eq ?_)
  · intro r hr _
    obtain ⟨b, hb⟩ := rewardsBaseReward_ok_mainnet tab r.validator htab_lo
      (hmax1 _ (by rw [← rowsOf_map_validator]; exact List.mem_map_of_mem hr))
    simp only [lhBaseReward, hb]
  · unfold lhPass
    rw [hcur1, ok_bind, htab, ok_bind, hctx, ok_bind, hpre, ok_bind]

/-! ## On the states at the end of an epoch -/

/-- Exit epochs are `u64` values. -/
def ExitBounds (s : BeaconState) : Prop :=
  ∀ v ∈ s.validators, v.exit_epoch ≤ FAR_FUTURE_EPOCH

/-- The supply facts of the epoch: each balance is below `2^62`, the effective balances sum to
less than `2^60` Gwei, and three times the slashed sum fits in a `u64`. -/
def EpochSupply (s : BeaconState) : Prop :=
  (∀ i, i < s.validators.length → s.balances.getD i 0 < 2 ^ 62) ∧
    totalEffectiveBalance s < 2 ^ 60 ∧ s.slashings.sum * 3 < 2 ^ 64

/-- `SameOk` is symmetric. -/
private theorem SameOk.symm' {α : Type} {x y : SpecM α} (h : SameOk x y) : SameOk y x :=
  fun v => (h v).symm

/-- On mainnet, at a state that `process_epoch` starts from, the Lighthouse epoch gives the
same `ok` results as the spec epoch. -/
theorem process_epoch_lh_sameOk' (o : Oracle) (t : BeaconState) (hb : BoundaryOk t)
    (hslot : t.slot < 2 ^ 64) (heb : EBOk Preset.mainnet t) (hexit : ExitBounds t)
    (hsupply : EpochSupply t) :
    SameOk (process_epoch_lh o t) (process_epoch Preset.mainnet o t) := by
  obtain ⟨hinv, hrows, hentry⟩ := hb
  obtain ⟨hbalance, htotal, hslashings⟩ := hsupply
  unfold process_epoch_lh
  split
  · exact SameOk.refl _
  rename_i hgen
  have hcurrent : get_current_epoch Preset.mainnet t =
      .ok (t.slot / Preset.mainnet.SLOTS_PER_EPOCH) := by
    simp [get_current_epoch, compute_epoch_at_slot, uint64Div, Preset.mainnet, pure,
      Except.pure]
  have hepoch : t.slot / Preset.mainnet.SLOTS_PER_EPOCH +
      Preset.mainnet.EPOCHS_PER_SLASHINGS_VECTOR / 2 < UINT64_SIZE := by
    have h64 : @LT.lt Nat _ t.slot (2 ^ 64) := hslot
    show @LT.lt Nat _ (t.slot / 32 + 4096) (2 ^ 64)
    omega
  exact SameOk.symm' (process_epoch_lh_sameOk o t hrows hinv _ hcurrent hgen hepoch hentry
    (fun i h => hexit _ (List.getElem_mem h)) (fun v hv => (heb v hv).2) hbalance htotal
    hslashings)


/-- `process_epoch_lh_sameOk'` in the form of the `hf` hypothesis of
`reachable_state_transition_sameOk'`, with `EpochSupply` as the epoch condition. -/
theorem process_epoch_lh_hf (o : Oracle) :
    ∀ t, EpochInputOk EpochSupply t →
      SameOk (process_epoch_lh o t) (process_epoch Preset.mainnet o t) :=
  fun t ⟨hb, hslot, heb, hexit, hsupply⟩ =>
    process_epoch_lh_sameOk' o t hb hslot heb hexit hsupply

end EpochProofs.Spec
