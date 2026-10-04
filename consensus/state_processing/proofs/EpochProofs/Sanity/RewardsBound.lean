import EpochProofs.Sanity.LighthouseRowOrder

/-!
# A weaker rewards condition for the Lighthouse row step

The inactivity penalty is the last of the four spec rounds, and its reward is zero. So the spec
order and the Lighthouse order agree when only the three flag penalties fit in the balance.
`lhRowStep_eq_singlePassStep_of_bounds` replaces the rewards condition with row facts that hold
during a long inactivity leak.
-/

namespace EpochProofs.Spec

/-- An addition that fits in a `u64` succeeds. -/
theorem uint64Add_of_lt (a b : Nat) (h : a + b < 2 ^ 64) : uint64Add a b = .ok (a + b) := by
  simp [uint64Add, UINT64_SIZE, h, pure, Except.pure]

/-- `saturating_sub` is truncated `Nat` subtraction. -/
theorem saturating_sub_eq_sub (a b : Nat) : saturating_sub a b = a - b := by
  unfold saturating_sub
  split
  · rfl
  · rename_i h
    show a - a = a - b
    have : ¬ b < a := h
    omega

/-- The Lighthouse order equals the spec order when the three flag penalties fit in the balance.
The inactivity penalty can be larger than the balance. `hpfit` is for the Lighthouse sum of
all four penalties, which uses a checked addition. -/
theorem rewardsCombined_eq_sequential_flags (balance : Nat) (s t h : Nat × Nat) (i : Nat)
    (hpenalties : s.2 + t.2 + h.2 ≤ balance)
    (hfit : balance + (s.1 + t.1 + h.1) < 2 ^ 64)
    (hpfit : s.2 + t.2 + h.2 + i < 2 ^ 64) :
    rewardsCombined balance [s, t, h, (0, i)] = rewardsSequential balance [s, t, h, (0, i)] := by
  obtain ⟨s1, s2⟩ := s
  obtain ⟨t1, t2⟩ := t
  obtain ⟨h1, h2⟩ := h
  dsimp only at hpenalties hfit hpfit
  simp (disch := omega) only [rewardsSequential, rewardsCombined, List.foldlM_cons,
    List.foldlM_nil, bind, Except.bind, pure, Except.pure, uint64Add_of_lt, saturating_sub_of_le]
  rw [saturating_sub_eq_sub, saturating_sub_eq_sub]
  congr 1
  show (balance + (0 + s1 + t1 + h1 + 0)) - (0 + s2 + t2 + h2 + i) =
    balance + s1 - s2 + t1 - t2 + h1 - h2 + 0 - i
  omega

/-- A bind that succeeds has a first step that succeeds. -/
theorem except_bind_ok {α β : Type} {x : SpecM α} {f : α → SpecM β} {y : β}
    (h : (x >>= f) = .ok y) : ∃ v, x = .ok v ∧ f v = .ok y := by
  cases x with
  | error e => cases h
  | ok v => exact ⟨v, rfl, h⟩

/-- A `u64` product that succeeds is the `Nat` product, and it fits. -/
theorem uint64Mul_ok_eq {a b c : Nat} (h : uint64Mul a b = .ok c) : c = a * b ∧ a * b < 2 ^ 64 := by
  unfold uint64Mul at h
  split at h
  · rename_i hlt
    cases h
    exact ⟨rfl, hlt⟩
  · cases h

/-- A `u64` quotient that succeeds is the `Nat` quotient, and the divisor is not zero. -/
theorem uint64Div_ok_eq {a b c : Nat} (h : uint64Div a b = .ok c) : c = a / b ∧ b ≠ 0 := by
  unfold uint64Div at h
  split at h
  · cases h
  · rename_i hne
    cases h
    exact ⟨rfl, hne⟩

/-- The penalty of one flag is at most `base * w / 64`, and the head flag has no penalty. If the
flag increments are at most `k` times the active increments, the reward is at most
`k * base * w / 64`. -/
theorem flagDelta_bounds (base w : Nat) (part head leak : Bool) (inc act k : Nat)
    (reward penalty : Nat)
    (h : flagDelta base w part head leak inc act = .ok (reward, penalty)) :
    64 * penalty ≤ base * w ∧ (head = true → penalty = 0) ∧
      (inc ≤ k * act → 64 * reward ≤ k * base * w) := by
  unfold flagDelta at h
  cases part <;> cases head <;> cases leak <;>
    simp only [Bool.not_true, Bool.not_false, if_true, if_false, Bool.false_eq_true] at h
  case false.false.false | false.false.true =>
    obtain ⟨m, hm, h⟩ := except_bind_ok h
    obtain ⟨q, hq, h⟩ := except_bind_ok h
    cases h
    obtain ⟨rfl, -⟩ := uint64Mul_ok_eq hm
    obtain ⟨rfl, -⟩ := uint64Div_ok_eq hq
    simp only [WEIGHT_DENOMINATOR]
    exact ⟨Nat.mul_div_le _ _, nofun, fun _ => by omega⟩
  case true.false.false | true.true.false =>
    obtain ⟨m, hm, h⟩ := except_bind_ok h
    obtain ⟨n, hn, h⟩ := except_bind_ok h
    obtain ⟨d, hd, h⟩ := except_bind_ok h
    obtain ⟨q, hq, h⟩ := except_bind_ok h
    cases h
    obtain ⟨rfl, -⟩ := uint64Mul_ok_eq hm
    obtain ⟨rfl, -⟩ := uint64Mul_ok_eq hn
    obtain ⟨rfl, -⟩ := uint64Mul_ok_eq hd
    obtain ⟨rfl, hne⟩ := uint64Div_ok_eq hq
    simp only [WEIGHT_DENOMINATOR] at hne ⊢
    have hact : 0 < act := by
      cases act with
      | zero => simp at hne
      | succ a => exact Nat.succ_pos a
    refine ⟨by omega, by simp, fun hk => ?_⟩
    have h1 := Nat.div_mul_le_self (base * w * inc) (act * 64)
    have h2 := Nat.mul_le_mul_left (base * w) hk
    have h3 : act * (64 * ((base * w * inc) / (act * 64))) ≤ act * (k * base * w) := by
      have e1 : act * (64 * ((base * w * inc) / (act * 64))) =
          (base * w * inc) / (act * 64) * (act * 64) := by grind
      have e2 : base * w * (k * act) = act * (k * base * w) := by grind
      omega
    exact Nat.le_of_mul_le_mul_left h3 hact
  all_goals cases h; omega

/-- The inactivity penalty times its denominator fits in a `u64`, because the numerator does. -/
theorem inactivityDelta_bound (p : Preset) (eb score : Nat) (target : Bool) (i : Nat)
    (h : inactivityDelta p eb score target = .ok i) :
    i * (p.INACTIVITY_SCORE_BIAS * p.INACTIVITY_PENALTY_QUOTIENT_BELLATRIX) < 2 ^ 64 := by
  unfold inactivityDelta at h
  cases target with
  | true =>
    cases h
    simp
  | false =>
    simp only [Bool.not_false, if_true] at h
    obtain ⟨m, hm, h⟩ := except_bind_ok h
    obtain ⟨d, hd, h⟩ := except_bind_ok h
    obtain ⟨rfl, hlt⟩ := uint64Mul_ok_eq hm
    obtain ⟨rfl, -⟩ := uint64Mul_ok_eq hd
    obtain ⟨rfl, -⟩ := uint64Div_ok_eq h
    exact Nat.lt_of_le_of_lt (Nat.div_mul_le_self _ _) hlt

/-- The four deltas have the shape `[s, t, h, (0, i)]`. The flag penalties are at most
`40 / 64` of the base reward (weights 14 and 26). If each flag has at most `k` times the active
increments, the flag rewards are at most `54 / 64` of `k` times the base reward. -/
theorem rewardDeltas_bounds (p : Preset) (base eb score : Nat) (source target head in_leak : Bool)
    (si ti hi ai : Nat) (ds : List (Gwei × Gwei))
    (h : rewardDeltas p base eb score source target head in_leak si ti hi ai = .ok ds) :
    ∃ s t h : Nat × Nat, ∃ i : Nat, ds = [s, t, h, (0, i)] ∧
      64 * (s.2 + t.2 + h.2) ≤ 40 * base ∧
      (∀ k, si ≤ k * ai → ti ≤ k * ai → hi ≤ k * ai → 64 * (s.1 + t.1 + h.1) ≤ 54 * (k * base)) ∧
      i * (p.INACTIVITY_SCORE_BIAS * p.INACTIVITY_PENALTY_QUOTIENT_BELLATRIX) < 2 ^ 64 := by
  unfold rewardDeltas at h
  obtain ⟨(⟨s1, s2⟩ : Nat × Nat), hs, h⟩ := except_bind_ok h
  obtain ⟨(⟨t1, t2⟩ : Nat × Nat), ht, h⟩ := except_bind_ok h
  obtain ⟨(⟨h1, h2⟩ : Nat × Nat), hh, h⟩ := except_bind_ok h
  obtain ⟨(i : Nat), hi', h⟩ := except_bind_ok h
  cases h
  refine ⟨(s1, s2), (t1, t2), (h1, h2), i, rfl, ?_, ?_, inactivityDelta_bound p _ _ _ _ hi'⟩
  · have bs := flagDelta_bounds _ _ _ _ _ _ _ 0 _ _ hs
    have bt := flagDelta_bounds _ _ _ _ _ _ _ 0 _ _ ht
    have bh := flagDelta_bounds _ _ _ _ _ _ _ 0 _ _ hh
    simp only [TIMELY_SOURCE_WEIGHT, TIMELY_TARGET_WEIGHT] at bs bt
    have := bh.2.1 rfl
    omega
  · intro k hks hkt hkh
    have bs := (flagDelta_bounds _ _ _ _ _ _ _ k _ _ hs).2.2 hks
    have bt := (flagDelta_bounds _ _ _ _ _ _ _ k _ _ ht).2.2 hkt
    have bh := (flagDelta_bounds _ _ _ _ _ _ _ k _ _ hh).2.2 hkh
    simp only [TIMELY_SOURCE_WEIGHT, TIMELY_TARGET_WEIGHT, TIMELY_HEAD_WEIGHT] at bs bt bh
    omega

/-- `4ab ≤ (a + b)²`. -/
theorem four_mul_le_sq (a b : Nat) : 4 * (a * b) ≤ (a + b) * (a + b) := by
  by_cases h : a ≤ b
  · obtain ⟨d, rfl⟩ := Nat.exists_eq_add_of_le h
    have : (a + (a + d)) * (a + (a + d)) = 4 * (a * (a + d)) + d * d := by grind
    omega
  · obtain ⟨d, rfl⟩ := Nat.exists_eq_add_of_le (Nat.le_of_not_le h)
    have : (b + d + b) * (b + d + b) = 4 * ((b + d) * b) + d * d := by grind
    omega

/-- One Newton step from a positive `y` stays at or above the floor square root of `n`. -/
theorem sqrt_step (n y : Nat) (hy : 0 < y) :
    n < ((y + n / y) / 2 + 1) * ((y + n / y) / 2 + 1) := by
  have hq : n < y * (n / y + 1) := by
    have := Nat.div_add_mod n y
    have := Nat.mod_lt n hy
    rw [Nat.mul_add, Nat.mul_one]; omega
  have h2 : y + (n / y + 1) ≤ 2 * ((y + n / y) / 2 + 1) := by omega
  have h3 := four_mul_le_sq y (n / y + 1)
  have h4 := Nat.mul_le_mul h2 h2
  have h5 : 2 * ((y + n / y) / 2 + 1) * (2 * ((y + n / y) / 2 + 1)) =
      4 * (((y + n / y) / 2 + 1) * ((y + n / y) / 2 + 1)) := by grind
  omega

/-- If both inputs of the loop are at or above the floor square root, the result is too. -/
theorem integer_squareroot_loop_spec (n : Nat) : ∀ x y : Nat, n < (x + 1) * (x + 1) →
    n < (y + 1) * (y + 1) →
    n < (integer_squareroot.loop n x y + 1) * (integer_squareroot.loop n x y + 1) := by
  intro x
  induction x using Nat.strongRecOn with
  | ind x ih =>
    intro y hx hy
    rw [integer_squareroot.loop.eq_1]
    split
    · rename_i hlt
      apply ih y hlt _ hy
      by_cases hy0 : y = 0
      · subst hy0
        simp only [Nat.zero_add, Nat.div_zero, Nat.zero_div]
        simp at hy
        omega
      · exact sqrt_step n y (Nat.pos_of_ne_zero hy0)
    · exact hx

/-- `integer_squareroot n` is at or above the floor square root of `n`. -/
theorem integer_squareroot_spec (n : Nat) :
    n < (integer_squareroot n + 1) * (integer_squareroot n + 1) := by
  unfold integer_squareroot
  split
  · rename_i h
    have : n = 2 ^ 64 - 1 := by simpa [UINT64_MAX] using h
    subst this
    decide
  · apply integer_squareroot_loop_spec
    · have := Nat.le_mul_self (n + 1)
      omega
    · by_cases hn0 : n = 0
      · subst hn0; decide
      · have := sqrt_step n n (Nat.pos_of_ne_zero hn0)
        rwa [Nat.div_self (Nat.pos_of_ne_zero hn0)] at this

/-- If `k * k ≤ n`, then `k ≤ integer_squareroot n`. -/
theorem le_integer_squareroot (n k : Nat) (h : k * k ≤ n) : k ≤ integer_squareroot n := by
  have hs := integer_squareroot_spec n
  obtain ⟨(r : Nat), hr⟩ : ∃ r : Nat, integer_squareroot n = r := ⟨_, rfl⟩
  rw [hr] at hs ⊢
  by_cases hk : r + 1 ≤ k
  · have := Nat.mul_self_le_mul_self hk
    omega
  · omega

/-- With an increment of 1 ETH and a base reward factor of 64, the base reward is at most
`1 / 256` of the effective balance. `htotal` holds on real states because
`get_total_active_balance` is at least one increment. The real ratio is about `1 / 494`. -/
theorem rewardsBaseReward_bound (p : Preset) (total_active_balance : Nat) (v : Validator)
    (base_reward : Nat) (hinc : p.EFFECTIVE_BALANCE_INCREMENT = 1000000000)
    (hfactor : p.BASE_REWARD_FACTOR = 64)
    (htotal : p.EFFECTIVE_BALANCE_INCREMENT ≤ total_active_balance)
    (h : rewardsBaseReward p total_active_balance v = .ok base_reward) :
    256 * base_reward ≤ v.effective_balance := by
  unfold rewardsBaseReward get_base_reward_per_increment at h
  rw [hinc, hfactor] at h
  have ht : (1000000000 : Nat) ≤ total_active_balance := hinc ▸ htotal
  obtain ⟨(m : Nat), hm, h⟩ := except_bind_ok h
  obtain ⟨(q : Nat), hq, h⟩ := except_bind_ok h
  obtain ⟨(f : Nat), hf, hq⟩ := except_bind_ok hq
  obtain ⟨rfl, -⟩ := uint64Div_ok_eq hm
  obtain ⟨rfl, -⟩ := uint64Mul_ok_eq hf
  obtain ⟨rfl, -⟩ := uint64Div_ok_eq hq
  obtain ⟨rfl, -⟩ := uint64Mul_ok_eq h
  have hroot : 31622 ≤ integer_squareroot total_active_balance :=
    le_integer_squareroot _ _ (Nat.le_trans (by decide) ht)
  obtain ⟨(r : Nat), hr⟩ : ∃ r : Nat, integer_squareroot total_active_balance = r := ⟨_, rfl⟩
  obtain ⟨(e : Nat), he⟩ : ∃ e : Nat, v.effective_balance = e := ⟨_, rfl⟩
  rw [hr] at hroot ⊢
  rw [he]
  have hper : 1000000000 * 64 / r ≤ 2023907 :=
    Nat.le_trans (Nat.div_le_div_left hroot (by decide)) (by decide)
  have := Nat.mul_le_mul_left (e / 1000000000) hper
  omega

/-- `rewardsBaseReward_bound` for the mainnet preset. -/
theorem rewardsBaseReward_bound_mainnet (total_active_balance : Nat) (v : Validator)
    (base_reward : Nat)
    (htotal : Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT ≤ total_active_balance)
    (h : rewardsBaseReward Preset.mainnet total_active_balance v = .ok base_reward) :
    256 * base_reward ≤ v.effective_balance :=
  rewardsBaseReward_bound _ _ _ _ rfl rfl htotal h

/-- `rewardsBaseReward_bound` for the minimal preset. -/
theorem rewardsBaseReward_bound_minimal (total_active_balance : Nat) (v : Validator)
    (base_reward : Nat)
    (htotal : Preset.minimal.EFFECTIVE_BALANCE_INCREMENT ≤ total_active_balance)
    (h : rewardsBaseReward Preset.minimal total_active_balance v = .ok base_reward) :
    256 * base_reward ≤ v.effective_balance :=
  rewardsBaseReward_bound _ _ _ _ rfl rfl htotal h

/-- `rewardDeltas_bounds` for the deltas of one row. `base` is the base reward of an eligible row,
or zero for a row that is not eligible. -/
theorem rewardsRowDeltas_bounds (p : Preset) (ctx : RewardsContext) (r : Row)
    (ds : List (Gwei × Gwei)) (h : rewardsRowDeltas p ctx r = .ok ds) :
    ∃ s t h : Nat × Nat, ∃ i base : Nat, ds = [s, t, h, (0, i)] ∧
      (base = 0 ∨ rewardsBaseReward p ctx.total_active_balance r.validator = .ok base) ∧
      64 * (s.2 + t.2 + h.2) ≤ 40 * base ∧
      (∀ k, ctx.source_increments ≤ k * ctx.active_increments →
        ctx.target_increments ≤ k * ctx.active_increments →
        ctx.head_increments ≤ k * ctx.active_increments →
        64 * (s.1 + t.1 + h.1) ≤ 54 * (k * base)) ∧
      i * (p.INACTIVITY_SCORE_BIAS * p.INACTIVITY_PENALTY_QUOTIENT_BELLATRIX) < 2 ^ 64 := by
  unfold rewardsRowDeltas at h
  obtain ⟨eligible, he, h⟩ := except_bind_ok h
  cases eligible with
  | false =>
    cases h
    exact ⟨(0, 0), (0, 0), (0, 0), 0, 0, rfl, .inl rfl, by decide, fun _ _ _ _ => by simp,
      by simp⟩
  | true =>
    obtain ⟨(base : Nat), hb, h⟩ := except_bind_ok h
    obtain ⟨s, t, hh, i, hds, hpen, hrew, hi⟩ := rewardDeltas_bounds _ _ _ _ _ _ _ _ _ _ _ _ _ h
    exact ⟨s, t, hh, i, base, hds, .inr hb, hpen, hrew, hi⟩

/-- The inactivity step keeps the validator and the balance. -/
theorem inactivityRowStep_keeps (p : Preset) (previous_epoch : Epoch) (in_leak : Bool) (r r' : Row)
    (h : inactivityRowStep p previous_epoch in_leak () r = .ok ((), r')) :
    r'.validator = r.validator ∧ r'.balance = r.balance := by
  unfold inactivityRowStep at h
  obtain ⟨eligible, -, h⟩ := except_bind_ok h
  cases eligible with
  | false => cases h; exact ⟨rfl, rfl⟩
  | true =>
    obtain ⟨score, -, h⟩ := except_bind_ok h
    cases h
    exact ⟨rfl, rfl⟩

/-- `lhRowStep_eq_singlePassStep` with a weaker rewards condition. `hrewards` asks only that the
three flag penalties fit in the balance, not the inactivity penalty. It also asks that the sum
of all four penalties fits in a `u64`, because Lighthouse adds them with a checked addition.
The other hypotheses are the same as in `lhRowStep_eq_singlePassStep`. -/
theorem lhRowStep_eq_singlePassStep_flags (p : Preset) (ctx : LhStepContext)
    (rctx : RewardsContext) (r : Row) (churn : Epoch × Gwei) (base_reward : Gwei)
    (activation_epoch : Epoch)
    (hprev : rctx.previous_epoch = ctx.previous_epoch)
    (hleak : rctx.in_leak = ctx.in_leak)
    (hsource : rctx.source_increments = ctx.source_increments)
    (htarget : rctx.target_increments = ctx.target_increments)
    (hhead : rctx.head_increments = ctx.head_increments)
    (hactive : rctx.active_increments = ctx.active_increments)
    (hbase : rewardsEligible ctx.previous_epoch r.validator = .ok true →
      rewardsBaseReward p rctx.total_active_balance r.validator = .ok base_reward)
    (hrewards : ∀ r' deltas,
      inactivityRowStep p ctx.previous_epoch ctx.in_leak () r = .ok ((), r') →
      rewardsRowDeltas p rctx r' = .ok deltas →
      ((deltas.take 3).map (·.2)).sum ≤ r.balance ∧
        r.balance + (deltas.map (·.1)).sum < 2 ^ 64 ∧ (deltas.map (·.2)).sum < 2 ^ 64)
    (hbalances : p.EJECTION_BALANCE < p.MIN_ACTIVATION_BALANCE)
    (hfinalized : ctx.finalized_epoch ≤ ctx.current_epoch)
    (hcurrent : ctx.current_epoch + 1 < UINT64_SIZE)
    (hactivation : compute_activation_exit_epoch p ctx.current_epoch = .ok activation_epoch)
    (hexit : r.validator.exit_epoch ≤ FAR_FUTURE_EPOCH) :
    lhRowStep p ctx base_reward churn r =
      (fun x => (x.1.1.2, x.2)) <$> singlePassStep p ctx.total_active_balance ctx.previous_epoch
        ctx.in_leak rctx ctx.current_epoch ctx.finalized_epoch activation_epoch
        ctx.slashings_target ctx.penalty_per_increment ((((), ()), churn), ()) r := by
  refine lhRowStep_eq_singlePassStep_of_combined p ctx rctx r churn base_reward activation_epoch
    hprev hleak hsource htarget hhead hactive hbase ?_ ?_ hbalances hfinalized hcurrent
    hactivation hexit
  · intro helig
    obtain ⟨rprev, rleak, rtotal, rsource, rtarget, rhead, ractive⟩ := rctx
    dsimp only at hprev hrewards
    subst hprev
    have hz := hrewards r [(0, 0), (0, 0), (0, 0), (0, 0)]
      (by simp [inactivityRowStep, validatorEligible_eq_rewardsEligible, helig, bind,
        Except.bind, pure, Except.pure])
      (by simp [rewardsRowDeltas, helig, bind, Except.bind, pure, Except.pure])
    simpa using hz.2.1
  · intro r' deltas hi hd
    have hc := hrewards r' deltas hi hd
    obtain ⟨s, t, h, i, -, hds, -⟩ := rewardsRowDeltas_bounds p rctx r' deltas hd
    subst hds
    simp only [List.take, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil] at hc
    obtain ⟨(b : Nat), hb⟩ : ∃ b : Nat, r.balance = b := ⟨_, rfl⟩
    rw [hb] at hc ⊢
    have h1 : s.2 + (t.2 + (h.2 + 0)) ≤ b := hc.1
    have h2 : b + (s.1 + (t.1 + (h.1 + (0 + 0)))) < 2 ^ 64 := hc.2.1
    have h3 : s.2 + (t.2 + (h.2 + (i + 0))) < 2 ^ 64 := hc.2.2
    exact rewardsCombined_eq_sequential_flags _ _ _ _ _ (by omega) (by omega) (by omega)

/-- The Lighthouse row step equals the spec row step on the mainnet preset, with row facts in
place of the rewards condition.

Hypotheses that replace `hrewards`, and why they hold on real states:
- `htotal`: `get_total_active_balance` is at least one increment.
- `hincrements`: each flag has at most 256 times the active increments. The flag increments
  count previous-epoch participants. The active increments count current-epoch active
  validators. The two sets differ only by the validators that exit or activate at this epoch
  boundary, and the churn limit keeps that change small.
- `heffective`: the effective balance is at most 256 times the balance. The hysteresis of
  the last effective balance update keeps the effective balance within 0.25 ETH above the
  balance, and the balance changes little between that update and this step.
- `hsupply`: the balance plus the effective balance fits in a `u64`. The total ETH supply is
  below `2 ^ 57` Gwei.

The other hypotheses are the same as in `lhRowStep_eq_singlePassStep`. `hbalances` is true on
mainnet, so it is not a hypothesis. -/
theorem lhRowStep_eq_singlePassStep_of_bounds (ctx : LhStepContext) (rctx : RewardsContext)
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
    (hincrements : ctx.source_increments ≤ 256 * ctx.active_increments ∧
      ctx.target_increments ≤ 256 * ctx.active_increments ∧
      ctx.head_increments ≤ 256 * ctx.active_increments)
    (heffective : r.validator.effective_balance ≤ 256 * r.balance)
    (hsupply : r.balance + r.validator.effective_balance < 2 ^ 64)
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
  obtain ⟨s, t, h, i, base, hds, hb, hpen, hrew, hinact⟩ :=
    rewardsRowDeltas_bounds _ _ _ _ hd
  rw [hv] at hb
  have hbase256 : 256 * base ≤ r.validator.effective_balance := by
    rcases hb with hb | hb
    · subst hb; exact Nat.zero_le _
    · exact rewardsBaseReward_bound_mainnet _ _ _ htotal hb
  have hr := hrew 256 (hsource ▸ hactive ▸ hincrements.1) (htarget ▸ hactive ▸ hincrements.2.1)
    (hhead ▸ hactive ▸ hincrements.2.2)
  have hq : Preset.mainnet.INACTIVITY_SCORE_BIAS *
      Preset.mainnet.INACTIVITY_PENALTY_QUOTIENT_BELLATRIX = 67108864 := rfl
  rw [hq] at hinact
  subst hds
  obtain ⟨(b : Nat), hbal⟩ : ∃ b : Nat, r.balance = b := ⟨_, rfl⟩
  obtain ⟨(e : Nat), heb⟩ : ∃ e : Nat, r.validator.effective_balance = e := ⟨_, rfl⟩
  rw [hbal] at heffective hsupply ⊢
  rw [heb] at heffective hsupply hbase256
  have he1 : e ≤ 256 * b := heffective
  have he2 : b + e < 2 ^ 64 := hsupply
  simp only [List.take, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  show s.2 + (t.2 + (h.2 + 0)) ≤ b ∧ b + (s.1 + (t.1 + (h.1 + (0 + 0)))) < 2 ^ 64 ∧
    s.2 + (t.2 + (h.2 + (i + 0))) < 2 ^ 64
  omega

end EpochProofs.Spec
