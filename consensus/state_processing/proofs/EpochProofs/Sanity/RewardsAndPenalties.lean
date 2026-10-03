import EpochProofs.Spec.RewardsAndPenalties

/-!
# Sanity theorems for the `process_rewards_and_penalties` reference

`flagDelta` and `inactivityDelta` are the loop bodies of `get_flag_index_deltas` and
`get_inactivity_penalty_deltas` for one eligible validator.

The spec applies the four deltas of a validator in four rounds: add the reward, then subtract
the penalty with `saturating_sub` (`rewardsSequential`). Lighthouse and Prysm add all rewards,
then subtract all penalties once (`rewardsCombined`).

`rewardsSequential_eq_combined`: the two agree when the balance covers all penalties and no
addition overflows. `rewards_saturation_example`: without that condition, they can differ.
-/

namespace EpochProofs.Spec

/-- The reward and the penalty of one eligible validator for one flag. -/
def flagDelta (base_reward weight : Uint64) (is_participating is_head_flag in_leak : Bool)
    (unslashed_participating_increments active_increments : Uint64) :
    SpecM (Uint64 × Uint64) :=
  if is_participating then
    if !in_leak then do
      let reward_numerator ←
        uint64Mul (← uint64Mul base_reward weight) unslashed_participating_increments
      let reward ← uint64Div reward_numerator (← uint64Mul active_increments WEIGHT_DENOMINATOR)
      pure (reward, 0)
    else pure (0, 0)
  else if !is_head_flag then do
    let penalty ← uint64Div (← uint64Mul base_reward weight) WEIGHT_DENOMINATOR
    pure (0, penalty)
  else pure (0, 0)

/-- The inactivity penalty of one eligible validator. -/
def inactivityDelta (p : Preset) (effective_balance inactivity_score : Uint64)
    (is_participating_target : Bool) : SpecM Uint64 :=
  if !is_participating_target then do
    let penalty_numerator ← uint64Mul effective_balance inactivity_score
    let penalty_denominator ←
      uint64Mul p.INACTIVITY_SCORE_BIAS p.INACTIVITY_PENALTY_QUOTIENT_BELLATRIX
    uint64Div penalty_numerator penalty_denominator
  else pure 0

/-- The spec: one round per delta, `increase_balance` then `decrease_balance`. -/
def rewardsSequential (balance : Gwei) (deltas : List (Gwei × Gwei)) : SpecM Gwei :=
  deltas.foldlM (fun b (reward, penalty) => do
    pure (saturating_sub (← uint64Add b reward) penalty)) balance

/-- Lighthouse: add all rewards, then subtract all penalties once. -/
def rewardsCombined (balance : Gwei) (deltas : List (Gwei × Gwei)) : SpecM Gwei := do
  let rewards ← deltas.foldlM (fun total (reward, _) => uint64Add total reward) 0
  let penalties ← deltas.foldlM (fun total (_, penalty) => uint64Add total penalty) 0
  pure (saturating_sub (← uint64Add balance rewards) penalties)

/-- The four deltas of one eligible validator, in spec order. -/
def rewardDeltas (p : Preset) (base_reward effective_balance inactivity_score : Uint64)
    (source target head in_leak : Bool)
    (source_increments target_increments head_increments active_increments : Uint64) :
    SpecM (List (Gwei × Gwei)) := do
  let s ← flagDelta base_reward TIMELY_SOURCE_WEIGHT source false in_leak source_increments
    active_increments
  let t ← flagDelta base_reward TIMELY_TARGET_WEIGHT target false in_leak target_increments
    active_increments
  let h ← flagDelta base_reward TIMELY_HEAD_WEIGHT head true in_leak head_increments
    active_increments
  let i ← inactivityDelta p effective_balance inactivity_score target
  pure [s, t, h, (0, i)]

theorem saturating_sub_of_le (a b : Nat) (h : b ≤ a) : saturating_sub a b = a - b := by
  unfold saturating_sub
  split
  · rfl
  · rename_i hn
    have : a = b := Nat.le_antisymm (Nat.le_of_not_gt hn) h
    subst this; rfl

theorem rewardsSequential_eq_combined (balance : Nat) (deltas : List (Nat × Nat))
    (hpenalties : (deltas.map (·.2)).sum ≤ balance)
    (hfit : balance + (deltas.map (·.1)).sum < 2 ^ 64) :
    rewardsSequential balance deltas =
      .ok (balance + (deltas.map (·.1)).sum - (deltas.map (·.2)).sum) := by
  induction deltas generalizing balance with
  | nil => simp [rewardsSequential, pure, Except.pure]
  | cons d ds ih =>
    obtain ⟨reward, penalty⟩ := d
    simp only [List.map_cons, List.sum_cons] at hpenalties hfit ⊢
    have hadd : uint64Add balance reward = .ok (balance + reward) := by
      have : balance + reward < 2 ^ 64 := by omega
      simp [uint64Add, UINT64_SIZE, this, pure, Except.pure]
    have hsub : saturating_sub (balance + reward) penalty = balance + reward - penalty :=
      saturating_sub_of_le _ _ (by omega)
    have hrest := ih (balance + reward - penalty) (by omega) (by omega)
    have hstep : rewardsSequential balance ((reward, penalty) :: ds) =
        rewardsSequential (balance + reward - penalty) ds := by
      simp only [rewardsSequential, List.foldlM_cons, bind, Except.bind, hadd, pure,
        Except.pure, hsub]
    rw [hstep, hrest]
    congr 1
    have key : ∀ (a d b e c : Nat), b + c ≤ a → a + d - b + e - c = a + (d + e) - (b + c) := by
      intros; omega
    exact key _ _ _ _ _ hpenalties

/-- Without the condition, the two orders differ: a balance of 3 with a source penalty of 5 and
a target reward of 10. -/
theorem rewards_saturation_example :
    rewardsSequential 3 [(0, 5), (10, 0)] = .ok 10 ∧
    rewardsCombined 3 [(0, 5), (10, 0)] = .ok 8 := by
  constructor <;> rfl

theorem foldlM_uint64Add_fst (l : List (Nat × Nat)) (acc : Nat)
    (hfit : acc + (l.map (·.1)).sum < 2 ^ 64) :
    l.foldlM (fun total (x : Gwei × Gwei) => uint64Add total x.1) acc =
      .ok (acc + (l.map (·.1)).sum) := by
  induction l generalizing acc with
  | nil => simp [pure, Except.pure]
  | cons d ds ih =>
    simp only [List.map_cons, List.sum_cons] at hfit ⊢
    have hadd : uint64Add acc d.1 = .ok (acc + d.1) := by
      have : acc + d.1 < 2 ^ 64 := by omega
      simp [uint64Add, UINT64_SIZE, this, pure, Except.pure]
    rw [List.foldlM_cons, hadd]
    simp only [bind, Except.bind]
    rw [ih (acc + d.1) (by omega)]
    congr 1
    exact Nat.add_assoc _ _ _

theorem foldlM_uint64Add_snd (l : List (Nat × Nat)) (acc : Nat)
    (hfit : acc + (l.map (·.2)).sum < 2 ^ 64) :
    l.foldlM (fun total (x : Gwei × Gwei) => uint64Add total x.2) acc =
      .ok (acc + (l.map (·.2)).sum) := by
  induction l generalizing acc with
  | nil => simp [pure, Except.pure]
  | cons d ds ih =>
    simp only [List.map_cons, List.sum_cons] at hfit ⊢
    have hadd : uint64Add acc d.2 = .ok (acc + d.2) := by
      have : acc + d.2 < 2 ^ 64 := by omega
      simp [uint64Add, UINT64_SIZE, this, pure, Except.pure]
    rw [List.foldlM_cons, hadd]
    simp only [bind, Except.bind]
    rw [ih (acc + d.2) (by omega)]
    congr 1
    exact Nat.add_assoc _ _ _

/-- When the balance covers all penalties and nothing overflows, the Lighthouse order equals
the spec order. -/
theorem rewardsCombined_eq_sequential (balance : Nat) (deltas : List (Nat × Nat))
    (hpenalties : (deltas.map (·.2)).sum ≤ balance)
    (hfit : balance + (deltas.map (·.1)).sum < 2 ^ 64) :
    rewardsCombined balance deltas = rewardsSequential balance deltas := by
  rw [rewardsSequential_eq_combined balance deltas hpenalties hfit]
  have hr := foldlM_uint64Add_fst deltas 0 (by omega)
  have hp := foldlM_uint64Add_snd deltas 0 (by omega)
  have hb : uint64Add balance (deltas.map (·.1)).sum =
      .ok (balance + (deltas.map (·.1)).sum) := by
    simp [uint64Add, UINT64_SIZE, hfit, pure, Except.pure]
  simp only [rewardsCombined, Nat.zero_add] at hr hp ⊢
  simp only [hr, hp, bind, Except.bind, hb, pure, Except.pure]
  rw [saturating_sub_of_le _ _ (by omega)]

end EpochProofs.Spec
