import EpochProofs.Sanity.Effects
import EpochProofs.Spec.Block.Withdrawals

/-!
# Effects of `process_withdrawals`

`process_withdrawals` keeps the registry and the checkpoints. A validator balance goes down only
to a floor: `MIN_ACTIVATION_BALANCE` for a pending partial withdrawal, the max effective balance
for a sweep partial withdrawal, or zero for a fully withdrawable validator. Builder withdrawals
change only the builders registry.
-/

namespace EpochProofs.Spec

/-- The total amount that `withdrawals` takes from index `i`. -/
def withdrawnAmount (withdrawals : List Withdrawal) (i : ValidatorIndex) : Gwei :=
  ((withdrawals.filter fun withdrawal => withdrawal.validator_index == i).map (·.amount)).sum

/-- The empty list withdraws nothing. -/
theorem withdrawnAmount_nil (i : ValidatorIndex) : withdrawnAmount [] i = 0 := rfl

/-- A withdrawal at the end adds its amount to its own index only. -/
theorem withdrawnAmount_append (ws : List Withdrawal) (w : Withdrawal) (i : ValidatorIndex) :
    withdrawnAmount (ws ++ [w]) i =
      withdrawnAmount ws i + (if w.validator_index = i then w.amount else 0) := by
  unfold withdrawnAmount
  by_cases h : w.validator_index = i
  · simp [h]
  · simp [h]

/-- A withdrawal at the front adds its amount to its own index only. -/
theorem withdrawnAmount_cons (w : Withdrawal) (ws : List Withdrawal) (i : ValidatorIndex) :
    withdrawnAmount (w :: ws) i =
      (if w.validator_index = i then w.amount else 0) + withdrawnAmount ws i := by
  unfold withdrawnAmount
  by_cases h : w.validator_index = i
  · simp [h]
  · simp [h]

/-- `saturating_sub` is `Nat` subtraction. -/
private theorem saturating_sub_eq (a b : Uint64) : saturating_sub a b = a - b := by
  unfold saturating_sub
  split
  · rfl
  · rename_i h
    rw [Nat.sub_self, Nat.sub_eq_zero_of_le (Nat.le_of_not_gt h)]

/-- `omega` on `Nat` atoms: a withdrawal that fits in the balance left. -/
private theorem withdraw_fits {old b a : Nat} (hle : old ≤ b) (ha : a ≤ b - old) :
    old + a ≤ b := by
  omega

/-- `get_current_epoch` is the slot divided by `SLOTS_PER_EPOCH`. -/
private theorem get_current_epoch_ok {p : Preset} {state : BeaconState} {epoch : Epoch}
    (h : get_current_epoch p state = .ok epoch) : epoch = state.slot / p.SLOTS_PER_EPOCH := by
  simp only [get_current_epoch, compute_epoch_at_slot, uint64Div] at h
  split at h
  · cases h
  · cases h; rfl

/-- A builder withdrawal index has the builder flag. -/
theorem is_builder_index_convert (builder_index : BuilderIndex) :
    is_builder_index (convert_builder_index_to_validator_index builder_index) = true := by
  have key : ∀ b n : Nat, (b ||| 2 ^ n) &&& 2 ^ n ≠ 0 := by
    intro b n h
    have := congrArg (fun x => x.testBit n) h
    simp only [Nat.testBit_and, Nat.testBit_or, Nat.testBit_two_pow_self, Nat.zero_testBit] at this
    simp at this
  simp only [is_builder_index, convert_builder_index_to_validator_index, BUILDER_INDEX_FLAG]
  exact bne_iff_ne.mpr (key builder_index 40)

/-- The balance of validator `i` after its withdrawals stays above a floor. -/
def WithdrawalFloor (p : Preset) (state : BeaconState) (i : ValidatorIndex) (r : Gwei) : Prop :=
  ∃ v : Validator, state.validators[i]? = some v ∧
    (p.MIN_ACTIVATION_BALANCE ≤ r ∨ get_max_effective_balance p v ≤ r ∨
      (r = 0 ∧ v.withdrawable_epoch ≤ state.slot / p.SLOTS_PER_EPOCH))

/-- Each validator index without the builder flag either has no withdrawn amount, or its
withdrawn amount fits in its balance and leaves the balance above a floor. -/
def WithdrawalsSafe (p : Preset) (state : BeaconState) (withdrawals : List Withdrawal) : Prop :=
  ∀ (i : ValidatorIndex) (b : Gwei), is_builder_index i = false → state.balances[i]? = some b →
    withdrawnAmount withdrawals i = 0 ∨
      (withdrawnAmount withdrawals i ≤ b ∧
        WithdrawalFloor p state i (b - withdrawnAmount withdrawals i))

/-- The empty list is safe. -/
theorem WithdrawalsSafe.nil (p : Preset) (state : BeaconState) : WithdrawalsSafe p state [] :=
  fun _ _ _ _ => Or.inl rfl

/-- A withdrawal with the builder flag keeps `WithdrawalsSafe`. -/
theorem WithdrawalsSafe.append_builder {p : Preset} {state : BeaconState}
    {ws : List Withdrawal} {w : Withdrawal} (hw : is_builder_index w.validator_index = true)
    (hs : WithdrawalsSafe p state ws) : WithdrawalsSafe p state (ws ++ [w]) := by
  intro i b hi hb
  have hne : w.validator_index ≠ i := by
    intro he
    rw [he, hi] at hw
    cases hw
  rw [withdrawnAmount_append, if_neg hne, Nat.add_zero]
  exact hs i b hi hb

/-- A validator withdrawal that leaves the balance above a floor keeps `WithdrawalsSafe`. -/
theorem WithdrawalsSafe.append_validator {p : Preset} {state : BeaconState}
    {ws : List Withdrawal} {w : Withdrawal} {bj : Gwei}
    (hs : WithdrawalsSafe p state ws) (hb : state.balances[w.validator_index]? = some bj)
    (hle : withdrawnAmount ws w.validator_index ≤ bj)
    (hamount : w.amount ≤ bj - withdrawnAmount ws w.validator_index)
    (hfloor : WithdrawalFloor p state w.validator_index
      (bj - withdrawnAmount ws w.validator_index - w.amount)) :
    WithdrawalsSafe p state (ws ++ [w]) := by
  intro i b hi hbi
  rw [withdrawnAmount_append]
  by_cases he : w.validator_index = i
  case neg =>
    rw [if_neg he, Nat.add_zero]
    exact hs i b hi hbi
  case pos =>
    subst he
    rw [if_pos rfl]
    rw [hb] at hbi
    cases hbi
    right
    refine ⟨withdraw_fits hle hamount, ?_⟩
    rw [← Nat.sub_sub]
    exact hfloor

/-- `get_balance_after_withdrawals` is the balance minus the withdrawn amount. -/
theorem get_balance_after_withdrawals_ok {state : BeaconState} {j : ValidatorIndex}
    {ws : List Withdrawal} {balance : Gwei}
    (h : get_balance_after_withdrawals state j ws = .ok balance) :
    ∃ bj, state.balances[j]? = some bj ∧ withdrawnAmount ws j ≤ bj ∧
      balance = bj - withdrawnAmount ws j := by
  unfold get_balance_after_withdrawals at h
  obtain ⟨withdrawn, h1, h⟩ := specM_bind_ok h
  obtain ⟨bj, h2, h⟩ := specM_bind_ok h
  have hw : withdrawn = withdrawnAmount ws j := by
    unfold uint64Sum at h1
    split at h1
    · cases h1; rfl
    · cases h1
  subst hw
  refine ⟨bj, ?_, ?_⟩
  · unfold listGet at h2
    split at h2
    · rename_i a ha
      cases h2
      exact ha
    · cases h2
  · unfold uint64Sub at h
    split at h
    · cases h
      exact ⟨by assumption, rfl⟩
    · cases h

/-- All withdrawals carry the builder flag. -/
def AllBuilder (withdrawals : List Withdrawal) : Prop :=
  ∀ w ∈ withdrawals, is_builder_index w.validator_index = true

/-- A withdrawal with the builder flag keeps `AllBuilder`. -/
theorem AllBuilder.append {ws : List Withdrawal} {w : Withdrawal} (hs : AllBuilder ws)
    (hw : is_builder_index w.validator_index = true) : AllBuilder (ws ++ [w]) := by
  intro x hx
  rcases List.mem_append.mp hx with hx | hx
  · exact hs x hx
  · rw [List.mem_singleton.mp hx]
    exact hw

/-- The builder withdrawals loop keeps `WithdrawalsSafe` and adds only builder withdrawals. -/
theorem get_builder_withdrawals_loop_safe (p : Preset) (state : BeaconState)
    (prior : List Withdrawal) (limit : Uint64) :
    ∀ (rest : List BuilderPendingWithdrawal) (wi pc : Nat) (ws out : List Withdrawal)
      (wi' pc' : Nat),
      get_builder_withdrawals_loop prior limit rest wi pc ws = .ok (out, wi', pc') →
      (WithdrawalsSafe p state (prior ++ ws) → WithdrawalsSafe p state (prior ++ out)) ∧
        (AllBuilder ws → AllBuilder out) := by
  intro rest
  induction rest with
  | nil =>
    intro wi pc ws out wi' pc' h
    cases h
    exact ⟨id, id⟩
  | cons w rest ih =>
    intro wi pc ws out wi' pc' h
    simp only [get_builder_withdrawals_loop] at h
    split at h
    · cases h
      exact ⟨id, id⟩
    · obtain ⟨_, -, h⟩ := specM_bind_ok h
      obtain ⟨_, -, h⟩ := specM_bind_ok h
      obtain ⟨_, -, h⟩ := specM_bind_ok h
      refine ⟨fun hs => (ih _ _ _ _ _ _ h).1 ?_,
        fun hb => (ih _ _ _ _ _ _ h).2 (hb.append (is_builder_index_convert _))⟩
      rw [← List.append_assoc]
      exact hs.append_builder (is_builder_index_convert _)

/-- A successful `listGet` reads the list. -/
private theorem listGet_ok {α : Type} {l : List α} {i : Nat} {a : α}
    (h : listGet l i = .ok a) :
    l[i]? = some a := by
  unfold listGet at h
  split at h
  · rename_i ha
    cases h
    exact ha
  · cases h

/-- A successful `uint64Sub` does not underflow. -/
private theorem uint64Sub_ok {a b c : Uint64} (h : uint64Sub a b = .ok c) :
    b ≤ a ∧ c = a - b := by
  unfold uint64Sub at h
  split at h
  · cases h
    exact ⟨by assumption, rfl⟩
  · cases h

/-- `omega` on `Nat` atoms: the pending partial amount leaves at least `MIN_ACTIVATION_BALANCE`. -/
private theorem pending_amount_fits {balance m a : Nat} (h : m < balance) :
    min (balance - m) a ≤ balance ∧ m ≤ balance - min (balance - m) a := by
  omega

/-- The pending partial withdrawals loop keeps `WithdrawalsSafe`. -/
theorem get_pending_partial_withdrawals_loop_safe (p : Preset) (state : BeaconState)
    (epoch : Epoch) (prior : List Withdrawal) (limit : Uint64) :
    ∀ (rest : List PendingPartialWithdrawal) (wi pc : Nat) (ws out : List Withdrawal)
      (wi' pc' : Nat),
      get_pending_partial_withdrawals_loop p state epoch prior limit rest wi pc ws =
        .ok (out, wi', pc') →
      WithdrawalsSafe p state (prior ++ ws) → WithdrawalsSafe p state (prior ++ out) := by
  intro rest
  induction rest with
  | nil =>
    intro wi pc ws out wi' pc' h hs
    cases h
    exact hs
  | cons w rest ih =>
    intro wi pc ws out wi' pc' h hs
    simp only [get_pending_partial_withdrawals_loop] at h
    split at h
    · cases h
      exact hs
    · obtain ⟨_, -, h⟩ := specM_bind_ok h
      obtain ⟨validator, hv, h⟩ := specM_bind_ok h
      obtain ⟨balance, hbal, h⟩ := specM_bind_ok h
      obtain ⟨bj, hbj, hle, hbeq⟩ := get_balance_after_withdrawals_ok hbal
      split at h
      · rename_i helig
        obtain ⟨d, hd, h⟩ := specM_bind_ok h
        obtain ⟨_, -, h⟩ := specM_bind_ok h
        obtain ⟨_, -, h⟩ := specM_bind_ok h
        obtain ⟨_, -, h⟩ := specM_bind_ok h
        refine ih _ _ _ _ _ _ h ?_
        rw [← List.append_assoc]
        simp only [is_eligible_for_partial_withdrawals, Bool.and_eq_true, decide_eq_true_eq]
          at helig
        obtain ⟨-, hd⟩ := uint64Sub_ok hd
        subst hd hbeq
        obtain ⟨h1, h2⟩ := pending_amount_fits (a := w.amount) helig.2
        exact hs.append_validator hbj hle h1 ⟨validator, listGet_ok hv, Or.inl h2⟩
      · obtain ⟨_, -, h⟩ := specM_bind_ok h
        obtain ⟨_, -, h⟩ := specM_bind_ok h
        exact ih _ _ _ _ _ _ h hs

/-- The builders sweep loop keeps `WithdrawalsSafe` and adds only builder withdrawals. -/
theorem get_builders_sweep_withdrawals_loop_safe (p : Preset) (state : BeaconState)
    (epoch : Epoch) (prior : List Withdrawal) (limit : Uint64) :
    ∀ (fuel builder_index wi pc : Nat) (ws out : List Withdrawal) (wi' pc' : Nat),
      get_builders_sweep_withdrawals_loop state epoch prior limit fuel builder_index wi pc ws =
        .ok (out, wi', pc') →
      (WithdrawalsSafe p state (prior ++ ws) → WithdrawalsSafe p state (prior ++ out)) ∧
        (AllBuilder ws → AllBuilder out) := by
  intro fuel
  induction fuel with
  | zero =>
    intro builder_index wi pc ws out wi' pc' h
    cases h
    exact ⟨id, id⟩
  | succ fuel ih =>
    intro builder_index wi pc ws out wi' pc' h
    simp only [get_builders_sweep_withdrawals_loop] at h
    split at h
    · cases h
      exact ⟨id, id⟩
    · obtain ⟨_, -, h⟩ := specM_bind_ok h
      obtain ⟨_, -, h⟩ := specM_bind_ok h
      split at h
      · repeat (obtain ⟨_, -, h⟩ := specM_bind_ok h)
        refine ⟨fun hs => (ih _ _ _ _ _ _ _ h).1 ?_,
          fun hb => (ih _ _ _ _ _ _ _ h).2 (hb.append (is_builder_index_convert builder_index))⟩
        rw [← List.append_assoc]
        exact hs.append_builder (is_builder_index_convert builder_index)
      · repeat (obtain ⟨_, -, h⟩ := specM_bind_ok h)
        exact ih _ _ _ _ _ _ _ h

/-- `omega` on `Nat` atoms: the sweep partial amount leaves the max effective balance. -/
private theorem sweep_amount_fits {balance m : Nat} (h : m < balance) :
    balance - m ≤ balance ∧ m ≤ balance - (balance - m) := by
  omega

/-- The validators sweep loop keeps `WithdrawalsSafe`. -/
theorem get_validators_sweep_withdrawals_loop_safe (p : Preset) (state : BeaconState)
    (epoch : Epoch) (hepoch : epoch = state.slot / p.SLOTS_PER_EPOCH) (prior : List Withdrawal)
    (limit : Uint64) :
    ∀ (fuel validator_index wi pc : Nat) (ws out : List Withdrawal) (wi' pc' : Nat),
      get_validators_sweep_withdrawals_loop p state epoch prior limit fuel validator_index wi pc
        ws = .ok (out, wi', pc') →
      WithdrawalsSafe p state (prior ++ ws) → WithdrawalsSafe p state (prior ++ out) := by
  intro fuel
  induction fuel with
  | zero =>
    intro validator_index wi pc ws out wi' pc' h hs
    cases h
    exact hs
  | succ fuel ih =>
    intro validator_index wi pc ws out wi' pc' h hs
    simp only [get_validators_sweep_withdrawals_loop] at h
    split at h
    · cases h
      exact hs
    · obtain ⟨_, -, h⟩ := specM_bind_ok h
      obtain ⟨validator, hv, h⟩ := specM_bind_ok h
      obtain ⟨balance, hbal, h⟩ := specM_bind_ok h
      obtain ⟨bj, hbj, hle, hbeq⟩ := get_balance_after_withdrawals_ok hbal
      subst hbeq
      split at h
      · rename_i hfull
        repeat (obtain ⟨_, -, h⟩ := specM_bind_ok h)
        refine ih _ _ _ _ _ _ _ h ?_
        rw [← List.append_assoc]
        simp only [is_fully_withdrawable_validator, Bool.and_eq_true, decide_eq_true_eq] at hfull
        refine hs.append_validator hbj hle (Nat.le_refl _) ⟨validator, listGet_ok hv, ?_⟩
        exact Or.inr (Or.inr ⟨Nat.sub_self _, hepoch ▸ hfull.1.2⟩)
      · split at h
        · rename_i _ hpartial
          obtain ⟨d, hd, h⟩ := specM_bind_ok h
          repeat (obtain ⟨_, -, h⟩ := specM_bind_ok h)
          refine ih _ _ _ _ _ _ _ h ?_
          rw [← List.append_assoc]
          simp only [is_partially_withdrawable_validator, Bool.and_eq_true, decide_eq_true_eq]
            at hpartial
          obtain ⟨-, hd⟩ := uint64Sub_ok hd
          subst hd
          obtain ⟨h1, h2⟩ := sweep_amount_fits hpartial.2
          exact hs.append_validator hbj hle h1 ⟨validator, listGet_ok hv, Or.inr (Or.inl h2)⟩
        · repeat (obtain ⟨_, -, h⟩ := specM_bind_ok h)
          exact ih _ _ _ _ _ _ _ h hs

/-- `get_builder_withdrawals` keeps `WithdrawalsSafe`. -/
theorem get_builder_withdrawals_safe {p : Preset} {state : BeaconState} {wi : Nat}
    {prior out : List Withdrawal} {wi' pc' : Nat}
    (h : get_builder_withdrawals p state wi prior = .ok (out, wi', pc'))
    (hs : WithdrawalsSafe p state prior) : WithdrawalsSafe p state (prior ++ out) := by
  unfold get_builder_withdrawals at h
  simp only [throw, throwThe, MonadExceptOf.throw] at h
  obtain ⟨limit, -, h⟩ := specM_bind_ok h
  split at h
  · cases h
  · obtain ⟨_, -, h⟩ := specM_bind_ok h
    exact (get_builder_withdrawals_loop_safe p state prior limit _ _ _ _ _ _ _ h).1
      (by rw [List.append_nil]; exact hs)

/-- `get_pending_partial_withdrawals` keeps `WithdrawalsSafe`. -/
theorem get_pending_partial_withdrawals_safe {p : Preset} {state : BeaconState} {wi : Nat}
    {prior out : List Withdrawal} {wi' pc' : Nat}
    (h : get_pending_partial_withdrawals p state wi prior = .ok (out, wi', pc'))
    (hs : WithdrawalsSafe p state prior) : WithdrawalsSafe p state (prior ++ out) := by
  unfold get_pending_partial_withdrawals at h
  simp only [throw, throwThe, MonadExceptOf.throw] at h
  obtain ⟨epoch, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  split at h
  · cases h
  · obtain ⟨_, -, h⟩ := specM_bind_ok h
    exact get_pending_partial_withdrawals_loop_safe p state epoch prior _ _ _ _ _ _ _ _ h
      (by rw [List.append_nil]; exact hs)

/-- `get_builders_sweep_withdrawals` keeps `WithdrawalsSafe`. -/
theorem get_builders_sweep_withdrawals_safe {p : Preset} {state : BeaconState} {wi : Nat}
    {prior out : List Withdrawal} {wi' pc' : Nat}
    (h : get_builders_sweep_withdrawals p state wi prior = .ok (out, wi', pc'))
    (hs : WithdrawalsSafe p state prior) : WithdrawalsSafe p state (prior ++ out) := by
  unfold get_builders_sweep_withdrawals at h
  simp only [throw, throwThe, MonadExceptOf.throw] at h
  obtain ⟨epoch, -, h⟩ := specM_bind_ok h
  obtain ⟨limit, -, h⟩ := specM_bind_ok h
  split at h
  · cases h
  · obtain ⟨_, -, h⟩ := specM_bind_ok h
    exact (get_builders_sweep_withdrawals_loop_safe p state epoch prior limit _ _ _ _ _ _ _ _ h).1
      (by rw [List.append_nil]; exact hs)

/-- `get_validators_sweep_withdrawals` keeps `WithdrawalsSafe`. -/
theorem get_validators_sweep_withdrawals_safe {p : Preset} {state : BeaconState} {wi : Nat}
    {prior out : List Withdrawal} {wi' pc' : Nat}
    (h : get_validators_sweep_withdrawals p state wi prior = .ok (out, wi', pc'))
    (hs : WithdrawalsSafe p state prior) : WithdrawalsSafe p state (prior ++ out) := by
  unfold get_validators_sweep_withdrawals at h
  simp only [throw, throwThe, MonadExceptOf.throw] at h
  obtain ⟨epoch, hepoch, h⟩ := specM_bind_ok h
  split at h
  · cases h
  · obtain ⟨_, -, h⟩ := specM_bind_ok h
    exact get_validators_sweep_withdrawals_loop_safe p state epoch
      (get_current_epoch_ok hepoch) prior _ _ _ _ _ _ _ _ _ h
      (by rw [List.append_nil]; exact hs)

/-- Every expected withdrawal list is safe for the validator balances. -/
theorem get_expected_withdrawals_safe {p : Preset} {state : BeaconState}
    {expected : ExpectedWithdrawals} (h : get_expected_withdrawals p state = .ok expected) :
    WithdrawalsSafe p state expected.withdrawals := by
  unfold get_expected_withdrawals at h
  obtain ⟨⟨w1, wi1, c1⟩, h1, h⟩ := specM_bind_ok h
  obtain ⟨⟨w2, wi2, c2⟩, h2, h⟩ := specM_bind_ok h
  obtain ⟨⟨w3, wi3, c3⟩, h3, h⟩ := specM_bind_ok h
  obtain ⟨⟨w4, wi4, c4⟩, h4, h⟩ := specM_bind_ok h
  cases h
  have s1 := get_builder_withdrawals_safe h1 (WithdrawalsSafe.nil p state)
  have s2 := get_pending_partial_withdrawals_safe h2 s1
  have s3 := get_builders_sweep_withdrawals_safe h3 s2
  exact get_validators_sweep_withdrawals_safe h4 s3

/-- A successful `decrease_balance` sets one balance to the old balance minus `delta`. -/
private theorem decrease_balance_ok {balances balances' : List Gwei} {j : ValidatorIndex}
    {a : Gwei}
    (h : decrease_balance balances j a = .ok balances') :
    ∃ bj, balances[j]? = some bj ∧ balances' = balances.set j (bj - a) := by
  unfold decrease_balance at h
  obtain ⟨bj, hbj, h⟩ := specM_bind_ok h
  refine ⟨bj, listGet_ok hbj, ?_⟩
  unfold listSet at h
  split at h
  · cases h
    rw [saturating_sub_eq]
  · cases h

/-- `apply_withdrawals` writes only `balances` and `builders`. Each validator index without the
builder flag loses its withdrawn amount. Each index with the flag keeps its balance. -/
theorem apply_withdrawals_ok :
    ∀ (withdrawals : List Withdrawal) (state state' : BeaconState),
      apply_withdrawals state withdrawals = .ok state' →
      (∃ (balances : List Gwei) (builders : List Builder),
        state' = { state with balances, builders }) ∧
      state'.balances.length = state.balances.length ∧
      ∀ (i : ValidatorIndex) (b : Gwei), state.balances[i]? = some b →
        state'.balances[i]? =
          some (if is_builder_index i then b else b - withdrawnAmount withdrawals i) := by
  intro withdrawals
  induction withdrawals with
  | nil =>
    intro state state' h
    cases h
    refine ⟨⟨state.balances, state.builders, rfl⟩, rfl, fun i b hb => ?_⟩
    rw [withdrawnAmount_nil, Nat.sub_zero, ite_self]
    exact hb
  | cons w rest ih =>
    intro state state' h
    simp only [apply_withdrawals] at h
    split at h
    · rename_i hw
      obtain ⟨_, -, h⟩ := specM_bind_ok h
      obtain ⟨builders, -, h⟩ := specM_bind_ok h
      obtain ⟨state1, h1, h⟩ := specM_bind_ok h
      cases h1
      obtain ⟨⟨bal, bld, he⟩, hlen, hbal⟩ := ih _ state' h
      refine ⟨⟨bal, bld, he⟩, hlen, fun i b hb => ?_⟩
      rw [hbal i b hb, withdrawnAmount_cons]
      by_cases hi : is_builder_index i = true
      · simp only [hi, if_true]
      · have hne : w.validator_index ≠ i := fun he => hi (he ▸ hw)
        simp only [hi, if_neg hne, Nat.zero_add]
    · rename_i hw
      obtain ⟨balances1, hd, h⟩ := specM_bind_ok h
      obtain ⟨state1, h1, h⟩ := specM_bind_ok h
      cases h1
      obtain ⟨⟨bal, bld, he⟩, hlen, hbal⟩ := ih _ state' h
      obtain ⟨bj, hbj, hset⟩ := decrease_balance_ok hd
      subst hset
      refine ⟨⟨bal, bld, he⟩, by rw [hlen, List.length_set], fun i b hb => ?_⟩
      rw [withdrawnAmount_cons]
      by_cases hne : w.validator_index = i
      · subst hne
        rw [hbj] at hb
        cases hb
        have hset : (state.balances.set w.validator_index (bj - w.amount))[w.validator_index]? =
            some (bj - w.amount) := by
          rw [List.getElem?_set_self', hbj]
          rfl
        rw [hbal _ _ hset, if_pos rfl]
        simp only [hw, Bool.false_eq_true, if_false, Nat.sub_sub]
      · have hset : (state.balances.set w.validator_index (bj - w.amount))[i]? = some b := by
          rw [List.getElem?_set_ne hne]
          exact hb
        rw [hbal _ _ hset, if_neg hne, Nat.zero_add]

/-- `s'` has the validators, balances, slot and checkpoints of `s`. -/
def WithdrawalsFrame (s s' : BeaconState) : Prop :=
  s'.validators = s.validators ∧ s'.balances = s.balances ∧ CheckpointsStable s s'

/-- Splits each `if` of an update step, then closes each success branch by `rfl` and each error
branch by `contradiction`. -/
macro "withdrawals_update_tac" h:ident : tactic => `(tactic| (
  simp only [bind, Except.bind, pure, Except.pure] at $h:ident
  repeat' split at $h:ident
  all_goals first
    | (cases $h:ident; exact ⟨rfl, rfl, rfl, rfl, rfl, rfl⟩)
    | contradiction))

/-- `update_next_withdrawal_index` keeps `WithdrawalsFrame`. -/
theorem update_next_withdrawal_index_frame {state state' : BeaconState}
    {withdrawals : List Withdrawal}
    (h : update_next_withdrawal_index state withdrawals = .ok state') :
    WithdrawalsFrame state state' := by
  unfold update_next_withdrawal_index at h
  withdrawals_update_tac h

/-- `update_next_withdrawal_builder_index` keeps `WithdrawalsFrame`. -/
theorem update_next_withdrawal_builder_index_frame {state state' : BeaconState} {count : Uint64}
    (h : update_next_withdrawal_builder_index state count = .ok state') :
    WithdrawalsFrame state state' := by
  unfold update_next_withdrawal_builder_index at h
  withdrawals_update_tac h

/-- `update_next_withdrawal_validator_index` keeps `WithdrawalsFrame`. -/
theorem update_next_withdrawal_validator_index_frame {p : Preset} {state state' : BeaconState}
    {withdrawals : List Withdrawal}
    (h : update_next_withdrawal_validator_index p state withdrawals = .ok state') :
    WithdrawalsFrame state state' := by
  unfold update_next_withdrawal_validator_index at h
  withdrawals_update_tac h

/-- `process_withdrawals` keeps validators, slot and checkpoints. The new balances are the old
balances minus the withdrawn amounts of one safe withdrawal list. -/
theorem process_withdrawals_core (p : Preset) (s s' : BeaconState)
    (h : process_withdrawals p s = .ok s') :
    s'.validators = s.validators ∧ CheckpointsStable s s' ∧
      s'.balances.length = s.balances.length ∧
      ∃ withdrawals : List Withdrawal, WithdrawalsSafe p s withdrawals ∧
        ∀ (i : ValidatorIndex) (b : Gwei), s.balances[i]? = some b →
          s'.balances[i]? =
            some (if is_builder_index i then b else b - withdrawnAmount withdrawals i) := by
  unfold process_withdrawals at h
  split at h
  · cases h
    refine ⟨rfl, CheckpointsStable.refl _, rfl, [], WithdrawalsSafe.nil p _, fun i b hb => ?_⟩
    rw [withdrawnAmount_nil, Nat.sub_zero, ite_self]
    exact hb
  · obtain ⟨_, -, h⟩ := specM_bind_ok h
    obtain ⟨expected, he, h⟩ := specM_bind_ok h
    obtain ⟨s1, h1, h⟩ := specM_bind_ok h
    obtain ⟨s2, h2, h⟩ := specM_bind_ok h
    obtain ⟨s3, h3, h⟩ := specM_bind_ok h
    obtain ⟨⟨bal, bld, hs1⟩, hlen, hbal⟩ := apply_withdrawals_ok _ _ _ h1
    obtain ⟨v2, b2, c2⟩ := update_next_withdrawal_index_frame h2
    obtain ⟨v3, b3, c3⟩ := update_next_withdrawal_builder_index_frame h3
    obtain ⟨v4, b4, c4⟩ := update_next_withdrawal_validator_index_frame h
    refine ⟨?_, ?_, ?_, expected.withdrawals, get_expected_withdrawals_safe he, ?_⟩
    · rw [v4.trans (v3.trans v2), hs1]
    · rw [hs1] at c2
      exact c2.trans (c3.trans c4)
    · rw [b4.trans (b3.trans b2)]
      exact hlen
    · intro i b hb
      rw [b4.trans (b3.trans b2)]
      exact hbal i b hb

/-- `process_withdrawals` keeps the effective balances. -/
theorem process_withdrawals_ebStable (p : Preset) (s s' : BeaconState)
    (h : process_withdrawals p s = .ok s') : EBStable s s' := by
  have hv := (process_withdrawals_core p s s' h).1
  exact ⟨by rw [hv]; exact Nat.le_refl _, fun i v hi => ⟨v, by rw [hv]; exact hi, rfl⟩⟩

/-- `process_withdrawals` keeps the slot and the checkpoints. -/
theorem process_withdrawals_checkpointsStable (p : Preset) (s s' : BeaconState)
    (h : process_withdrawals p s = .ok s') : CheckpointsStable s s' :=
  (process_withdrawals_core p s s' h).2.1

/-- `process_withdrawals` keeps `ExitOrder`. -/
theorem process_withdrawals_exitOrder (p : Preset) (s s' : BeaconState)
    (h : process_withdrawals p s = .ok s') (hs : ExitOrder s) : ExitOrder s' := by
  rw [ExitOrder, (process_withdrawals_core p s s' h).1]
  exact hs

/-- A validator balance either stays the same or goes down to a floor:
`MIN_ACTIVATION_BALANCE` (pending partial withdrawal), the max effective balance (sweep partial
withdrawal), or zero for a validator that is withdrawable at the current epoch (full
withdrawal). With several withdrawals for one validator, the last one sets the floor. -/
theorem process_withdrawals_balances (p : Preset) (s s' : BeaconState)
    (h : process_withdrawals p s = .ok s') :
    s'.balances.length = s.balances.length ∧
      ∀ (i : ValidatorIndex) (b : Gwei), s.balances[i]? = some b →
        ∃ b' : Gwei, s'.balances[i]? = some b' ∧ b' ≤ b ∧
          (b' = b ∨ ∃ v : Validator, s.validators[i]? = some v ∧
            (p.MIN_ACTIVATION_BALANCE ≤ b' ∨ get_max_effective_balance p v ≤ b' ∨
              (b' = 0 ∧ v.withdrawable_epoch ≤ s.slot / p.SLOTS_PER_EPOCH))) := by
  obtain ⟨-, -, hlen, withdrawals, hsafe, hbal⟩ := process_withdrawals_core p s s' h
  refine ⟨hlen, fun i b hb => ?_⟩
  rw [hbal i b hb]
  cases hi : is_builder_index i
  case true =>
    exact ⟨b, rfl, Nat.le_refl _, Or.inl rfl⟩
  case false =>
    refine ⟨b - withdrawnAmount withdrawals i, rfl, Nat.sub_le _ _, ?_⟩
    rcases hsafe i b hi hb with h0 | ⟨-, hfloor⟩
    · left
      rw [h0, Nat.sub_zero]
    · exact Or.inr hfloor

/-- Builder withdrawals never change a validator balance: an index with the builder flag keeps
its balance, and every builder withdrawal carries the flag. -/
theorem process_withdrawals_builder_index_balance (p : Preset) (s s' : BeaconState)
    (h : process_withdrawals p s = .ok s') (i : ValidatorIndex)
    (hi : is_builder_index i = true) : s'.balances[i]? = s.balances[i]? := by
  obtain ⟨-, -, -, withdrawals, -, hbal⟩ := process_withdrawals_core p s s' h
  cases hb : s.balances[i]? with
  | some b =>
    rw [hbal i b hb, hi, if_pos rfl]
  | none =>
    obtain ⟨-, -, hlen, -⟩ := process_withdrawals_core p s s' h
    rw [List.getElem?_eq_none_iff] at hb ⊢
    omega

/-- The builder pending withdrawals produce only withdrawals with the builder flag. -/
theorem get_builder_withdrawals_allBuilder {p : Preset} {state : BeaconState} {wi : Nat}
    {prior out : List Withdrawal} {wi' pc' : Nat}
    (h : get_builder_withdrawals p state wi prior = .ok (out, wi', pc')) : AllBuilder out := by
  unfold get_builder_withdrawals at h
  simp only [throw, throwThe, MonadExceptOf.throw] at h
  obtain ⟨limit, -, h⟩ := specM_bind_ok h
  split at h
  · cases h
  · obtain ⟨_, -, h⟩ := specM_bind_ok h
    exact (get_builder_withdrawals_loop_safe p state prior limit _ _ _ _ _ _ _ h).2
      (fun _ hw => nomatch hw)

/-- The builders sweep produces only withdrawals with the builder flag. -/
theorem get_builders_sweep_withdrawals_allBuilder {p : Preset} {state : BeaconState} {wi : Nat}
    {prior out : List Withdrawal} {wi' pc' : Nat}
    (h : get_builders_sweep_withdrawals p state wi prior = .ok (out, wi', pc')) :
    AllBuilder out := by
  unfold get_builders_sweep_withdrawals at h
  simp only [throw, throwThe, MonadExceptOf.throw] at h
  obtain ⟨epoch, -, h⟩ := specM_bind_ok h
  obtain ⟨limit, -, h⟩ := specM_bind_ok h
  split at h
  · cases h
  · obtain ⟨_, -, h⟩ := specM_bind_ok h
    exact (get_builders_sweep_withdrawals_loop_safe p state epoch prior limit _ _ _ _ _ _ _ _ h).2
      (fun _ hw => nomatch hw)

end EpochProofs.Spec
