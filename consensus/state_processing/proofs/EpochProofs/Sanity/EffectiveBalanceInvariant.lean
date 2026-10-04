import EpochProofs.Sanity.RowsEffectiveBalance

/-!
# Effective balance floor after `process_effective_balance_updates`

After the effective balance updates, each effective balance is a multiple of the increment and
at most `balance + DOWNWARD_THRESHOLD`. On mainnet this gives `3 * effective_balance ≤ 4 *
balance`. `EffectiveBalanceFloor` states this for the whole state.
-/

namespace EpochProofs.Spec

/-- The two maximum effective balances are multiples of the increment. -/
def PresetIncrementOk (p : Preset) : Prop :=
  p.MIN_ACTIVATION_BALANCE % p.EFFECTIVE_BALANCE_INCREMENT = 0
  ∧ p.MAX_EFFECTIVE_BALANCE_ELECTRA % p.EFFECTIVE_BALANCE_INCREMENT = 0

/-- Mainnet holds `PresetIncrementOk`. -/
theorem presetIncrementOk_mainnet : PresetIncrementOk Preset.mainnet :=
  ⟨rfl, rfl⟩

/-- `get_max_effective_balance` is a multiple of the increment. -/
theorem get_max_effective_balance_mod (p : Preset) (hp : PresetIncrementOk p)
    (validator : Validator) :
    get_max_effective_balance p validator % p.EFFECTIVE_BALANCE_INCREMENT = 0 := by
  unfold get_max_effective_balance
  split
  · exact hp.2
  · exact hp.1

/-- The new effective balance either keeps the old one inside the downward band, or is the
rounded balance capped by the maximum. -/
theorem newEffectiveBalance_cases (p : Preset) (downward upward : Uint64)
    (validator : Validator) (balance effective_balance : Gwei)
    (h : newEffectiveBalance p downward upward validator balance = .ok effective_balance) :
    (effective_balance = validator.effective_balance
      ∧ validator.effective_balance ≤ balance + downward)
    ∨ effective_balance = min (balance - balance % p.EFFECTIVE_BALANCE_INCREMENT)
        (get_max_effective_balance p validator) := by
  have hround : ∀ x : Gwei, (do
      let remainder ← uint64Mod balance p.EFFECTIVE_BALANCE_INCREMENT
      let rounded ← uint64Sub balance remainder
      pure (min rounded (get_max_effective_balance p validator)) : SpecM Gwei) = .ok x →
      x = min (balance - balance % p.EFFECTIVE_BALANCE_INCREMENT)
        (get_max_effective_balance p validator) := by
    intro x hx
    by_cases h0 : p.EFFECTIVE_BALANCE_INCREMENT = 0
    · simp [uint64Mod, h0, bind, Except.bind, throw, throwThe, MonadExceptOf.throw] at hx
    · simp [uint64Mod, uint64Sub, h0, Nat.mod_le, bind, Except.bind, pure, Except.pure] at hx
      exact hx.symm
  unfold newEffectiveBalance at h
  by_cases h1 : balance + downward < UINT64_SIZE
  case neg =>
    simp [uint64Add, h1, bind, Except.bind, throw, throwThe, MonadExceptOf.throw] at h
  by_cases h2 : balance + downward < validator.effective_balance
  · simp only [uint64Add, h1, h2, if_true, bind, Except.bind, pure, Except.pure] at h
    exact Or.inr (hround _ h)
  by_cases h3 : validator.effective_balance + upward < UINT64_SIZE
  case neg =>
    simp [uint64Add, h1, h2, h3, bind, Except.bind, pure, Except.pure, throw, throwThe,
      MonadExceptOf.throw] at h
  by_cases h4 : validator.effective_balance + upward < balance
  · simp only [uint64Add, h1, h2, h3, h4, if_true, if_false, bind, Except.bind, pure,
      Except.pure, decide_true] at h
    exact Or.inr (hround _ h)
  · simp [uint64Add, h1, h2, h3, h4, bind, Except.bind, pure, Except.pure] at h
    exact Or.inl ⟨h.symm, Nat.le_of_not_lt h2⟩

/-- Bounds on one new effective balance. The capped branch needs `PresetIncrementOk` for the
multiple. The unchanged branch needs the old effective balance to be a multiple. The other two
bounds need no hypothesis. -/
theorem newEffectiveBalance_bounds (p : Preset) (hp : PresetIncrementOk p)
    (downward upward : Uint64) (validator : Validator) (balance effective_balance : Gwei)
    (h : newEffectiveBalance p downward upward validator balance = .ok effective_balance)
    (hmod : validator.effective_balance % p.EFFECTIVE_BALANCE_INCREMENT = 0) :
    effective_balance % p.EFFECTIVE_BALANCE_INCREMENT = 0
    ∧ effective_balance ≤ balance + downward
    ∧ effective_balance ≤ max validator.effective_balance
        (get_max_effective_balance p validator) := by
  rcases newEffectiveBalance_cases p downward upward validator balance effective_balance h with
    ⟨rfl, hle⟩ | rfl
  · exact ⟨hmod, hle, Nat.le_max_left _ _⟩
  · refine ⟨?_, ?_, Nat.le_trans (Nat.min_le_right _ _) (Nat.le_max_right _ _)⟩
    · rcases Nat.le_total (balance - balance % p.EFFECTIVE_BALANCE_INCREMENT)
        (get_max_effective_balance p validator) with hle | hle
      · rw [Nat.min_eq_left hle]
        exact Nat.sub_mod_eq_zero_of_mod_eq (Nat.mod_mod _ _).symm
      · rw [Nat.min_eq_right hle]
        exact get_max_effective_balance_mod p hp validator
    · exact Nat.le_trans (Nat.min_le_left _ _)
        (Nat.le_trans (Nat.sub_le _ _) (Nat.le_add_right _ _))

/-- A multiple of `increment` at most `balance + downward` is at most `4/3` of `balance`, if
`downward` is at most a quarter of `increment`. -/
theorem three_mul_le_of_floor (increment downward effective_balance balance : Nat)
    (hmod : effective_balance % increment = 0)
    (hle : effective_balance ≤ balance + downward)
    (hdown : 4 * downward ≤ increment) :
    3 * effective_balance ≤ 4 * balance := by
  by_cases h0 : effective_balance = 0
  · omega
  · have hpos : 0 < effective_balance := Nat.pos_of_ne_zero h0
    have hinc : increment ≤ effective_balance :=
      Nat.le_of_dvd hpos (Nat.dvd_of_mod_eq_zero hmod)
    omega

/-- `3 * a ≤ 4 * b` gives `a ≤ 256 * b`. -/
theorem le_mul_of_three_mul_le {a b : Nat} (h : 3 * a ≤ 4 * b) : a ≤ 256 * b := by
  omega

/-- On mainnet, a new effective balance is at most `4/3` of the balance, and so at most 256
times the balance. -/
theorem newEffectiveBalance_mainnet_floor (validator : Validator)
    (balance effective_balance : Gwei)
    (h : newEffectiveBalance Preset.mainnet 250000000 1250000000 validator balance =
      .ok effective_balance)
    (hmod : validator.effective_balance % Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT = 0) :
    3 * effective_balance ≤ 4 * balance ∧ effective_balance ≤ 256 * balance := by
  obtain ⟨hm, hle, -⟩ := newEffectiveBalance_bounds Preset.mainnet presetIncrementOk_mainnet
    _ _ validator balance effective_balance h hmod
  have h3 := three_mul_le_of_floor Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT 250000000
    effective_balance balance hm hle (by decide)
  exact ⟨h3, le_mul_of_three_mul_le h3⟩

/-- If `mapM` succeeds, each output is `f` of the input at the same index. -/
theorem mapM_ok_getElem {α β : Type} (f : α → SpecM β) :
    ∀ (l : List α) (ys : List β), l.mapM f = .ok ys →
      ys.length = l.length ∧ ∀ i (hl : i < l.length) (hy : i < ys.length), f l[i] = .ok ys[i]
  | [], ys, h => by
    simp only [List.mapM_nil, pure, Except.pure, Except.ok.injEq] at h
    subst h
    exact ⟨rfl, fun i hl => absurd hl (Nat.not_lt_zero _)⟩
  | x :: xs, ys, h => by
    rw [List.mapM_cons] at h
    cases hx : f x with
    | error e => simp [hx, bind, Except.bind] at h
    | ok y =>
      cases hxs : xs.mapM f with
      | error e => simp [hx, hxs, bind, Except.bind] at h
      | ok ys' =>
        simp only [hx, hxs, bind, Except.bind, pure, Except.pure, Except.ok.injEq] at h
        subst h
        obtain ⟨hlen, hget⟩ := mapM_ok_getElem f xs ys' hxs
        refine ⟨by simp [hlen], fun i hl hy => ?_⟩
        cases i with
        | zero => exact hx
        | succ i =>
          exact hget i (Nat.lt_of_succ_lt_succ hl) (Nat.lt_of_succ_lt_succ hy)

/-- One successful loop step reads the balance at its index and applies
`newEffectiveBalance`. -/
theorem effectiveBalanceStep_ok (p : Preset) (balances : List Gwei) (downward upward : Uint64)
    (hthresholds : hysteresisThresholds p = .ok (downward, upward)) (x : Validator × Nat)
    (validator : Validator) (h : effectiveBalanceStep p balances x = .ok validator) :
    ∃ balance, balances[x.2]? = some balance
      ∧ newEffectiveBalance p downward upward x.1 balance = .ok validator.effective_balance := by
  unfold effectiveBalanceStep at h
  cases hb : balances[x.2]? with
  | none => simp [hb, bind, Except.bind, throw, throwThe, MonadExceptOf.throw] at h
  | some balance =>
    refine ⟨balance, rfl, ?_⟩
    simp only [hb, hthresholds, bind, Except.bind, pure, Except.pure] at h
    cases hn : newEffectiveBalance p downward upward x.1 balance with
    | error e => simp [hn] at h
    | ok b =>
      simp only [hn, Except.ok.injEq] at h
      subst h
      rfl

/-- After `process_effective_balance_updates`, each effective balance is a multiple of the
increment and at most `balance + downward`. Balances do not change. A successful run reads a
balance for each validator, so no `RowsOk` hypothesis is necessary. -/
theorem process_effective_balance_updates_bounds (p : Preset) (hp : PresetIncrementOk p)
    (downward upward : Uint64) (hthresholds : hysteresisThresholds p = .ok (downward, upward))
    (state state' : BeaconState) (h : process_effective_balance_updates p state = .ok state')
    (hmod : ∀ v ∈ state.validators, v.effective_balance % p.EFFECTIVE_BALANCE_INCREMENT = 0) :
    state'.balances = state.balances
    ∧ state'.validators.length = state.validators.length
    ∧ state'.validators.length ≤ state'.balances.length
    ∧ ∀ i (hi : i < state'.validators.length),
        state'.validators[i].effective_balance % p.EFFECTIVE_BALANCE_INCREMENT = 0
        ∧ state'.validators[i].effective_balance ≤ state'.balances.getD i 0 + downward := by
  rw [process_effective_balance_updates_eq] at h
  cases hm : state.validators.zipIdx.mapM (effectiveBalanceStep p state.balances) with
  | error e => simp [hm, Except.map] at h
  | ok validators =>
    simp only [hm, Except.map, Except.ok.injEq] at h
    subst h
    obtain ⟨hlen, hget⟩ := mapM_ok_getElem _ _ _ hm
    rw [List.length_zipIdx] at hlen
    have hstep : ∀ i (hi : i < validators.length), ∃ balance,
        state.balances[i]? = some balance
        ∧ validators[i].effective_balance % p.EFFECTIVE_BALANCE_INCREMENT = 0
        ∧ validators[i].effective_balance ≤ balance + downward := by
      intro i hi
      have hv : i < state.validators.length := hlen ▸ hi
      have hs := hget i (by simpa using hv) hi
      rw [List.getElem_zipIdx] at hs
      obtain ⟨balance, hb, hn⟩ :=
        effectiveBalanceStep_ok p state.balances downward upward hthresholds _ _ hs
      simp only [Nat.zero_add] at hb hn
      obtain ⟨hm', hle, -⟩ := newEffectiveBalance_bounds p hp downward upward _ balance _ hn
        (hmod _ (List.getElem_mem hv))
      exact ⟨balance, hb, hm', hle⟩
    refine ⟨rfl, hlen, ?_, fun i hi => ?_⟩
    · cases hv : validators.length with
      | zero => exact Nat.zero_le _
      | succ n =>
        obtain ⟨balance, hb, -⟩ := hstep n (by omega)
        have := (List.getElem?_eq_some_iff.mp hb).1
        show n + 1 ≤ state.balances.length
        omega
    · obtain ⟨balance, hb, hm', hle⟩ := hstep i hi
      simp only [List.getD_eq_getElem?_getD, hb, Option.getD_some]
      exact ⟨hm', hle⟩

/-- Every validator has a balance, and every effective balance is a multiple of the increment
and at most `4/3` of the balance. -/
def EffectiveBalanceFloor (p : Preset) (state : BeaconState) : Prop :=
  state.validators.length ≤ state.balances.length
  ∧ ∀ i (hi : i < state.validators.length),
      state.validators[i].effective_balance % p.EFFECTIVE_BALANCE_INCREMENT = 0
      ∧ 3 * state.validators[i].effective_balance ≤ 4 * state.balances.getD i 0

/-- Under `EffectiveBalanceFloor`, each effective balance is at most 256 times the balance. -/
theorem EffectiveBalanceFloor.le_mul (p : Preset) (state : BeaconState)
    (h : EffectiveBalanceFloor p state) (i : Nat) (hi : i < state.validators.length) :
    state.validators[i].effective_balance ≤ 256 * state.balances.getD i 0 := by
  exact le_mul_of_three_mul_le (h.2 i hi).2

/-- `process_effective_balance_updates` establishes `EffectiveBalanceFloor`, if the downward
threshold is at most a quarter of the increment. -/
theorem process_effective_balance_updates_floor (p : Preset) (hp : PresetIncrementOk p)
    (downward upward : Uint64) (hthresholds : hysteresisThresholds p = .ok (downward, upward))
    (hdown : 4 * downward ≤ p.EFFECTIVE_BALANCE_INCREMENT)
    (state state' : BeaconState) (h : process_effective_balance_updates p state = .ok state')
    (hmod : ∀ v ∈ state.validators, v.effective_balance % p.EFFECTIVE_BALANCE_INCREMENT = 0) :
    EffectiveBalanceFloor p state' := by
  obtain ⟨-, -, hlen, hbounds⟩ :=
    process_effective_balance_updates_bounds p hp downward upward hthresholds state state' h hmod
  refine ⟨hlen, fun i hi => ?_⟩
  obtain ⟨hm, hle⟩ := hbounds i hi
  exact ⟨hm, three_mul_le_of_floor _ downward _ _ hm hle hdown⟩

/-- On mainnet, `process_effective_balance_updates` establishes `EffectiveBalanceFloor`. -/
theorem process_effective_balance_updates_floor_mainnet (state state' : BeaconState)
    (h : process_effective_balance_updates Preset.mainnet state = .ok state')
    (hmod : ∀ v ∈ state.validators,
      v.effective_balance % Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT = 0) :
    EffectiveBalanceFloor Preset.mainnet state' :=
  process_effective_balance_updates_floor Preset.mainnet presetIncrementOk_mainnet _ _
    hysteresisThresholds_mainnet (by decide) state state' h hmod

end EpochProofs.Spec
