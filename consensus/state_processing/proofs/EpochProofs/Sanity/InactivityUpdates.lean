import EpochProofs.Spec.InactivityUpdates

/-!
# Sanity theorems for the `process_inactivity_updates` reference

Facts about the reference alone. If a transcription error breaks a fact, the build fails.

The Lighthouse proof targets `inactivityScoreStep`. `process_inactivity_updates_eq` shows that
the loop applies it to each eligible validator.
-/

namespace EpochProofs.Spec

/-- The new inactivity score of one eligible validator. -/
def inactivityScoreStep (p : Preset) (participating in_leak : Bool) (score : Uint64) :
    SpecM Uint64 := do
  let score ← if participating then pure (saturating_sub score 1)
    else uint64Add score p.INACTIVITY_SCORE_BIAS
  pure (if in_leak then score else saturating_sub score p.INACTIVITY_SCORE_RECOVERY_RATE)

/-- One iteration of the loop, with the participating set and the leak flag already known. -/
def inactivityLoopStep (p : Preset) (participating : List ValidatorIndex) (in_leak : Bool)
    (scores : List Uint64) (index : ValidatorIndex) : SpecM (List Uint64) := do
  let score ← listGet scores index
  listSet scores index (← inactivityScoreStep p (index ∈ participating) in_leak score)

theorem listGet_ok {α : Type} {l : List α} {i : Nat} {a : α} (h : listGet l i = .ok a) :
    i < l.length ∧ l[i]? = some a := by
  unfold listGet at h
  cases hl : l[i]? with
  | none => simp [hl] at h
  | some b =>
    simp only [hl] at h
    cases h
    exact ⟨(List.getElem?_eq_some_iff.mp hl).1, rfl⟩

theorem listSet_of_lt {α : Type} {l : List α} {i : Nat} (a : α) (h : i < l.length) :
    listSet l i a = .ok (l.set i a) := by
  simp [listSet, h, pure, Except.pure]

theorem listGet_set {α : Type} {l : List α} {i : Nat} (a : α) (h : i < l.length) :
    listGet (l.set i a) i = .ok a := by
  simp [listGet, h, pure, Except.pure]

theorem forIn_yield_eq_foldlM {α β : Type} (l : List α) (acc : β)
    (body : α → β → SpecM (ForInStep β)) (step : β → α → SpecM β)
    (h : ∀ a b, body a b = match step b a with
      | .error e => .error e
      | .ok v => .ok (ForInStep.yield v)) :
    forIn l acc body = l.foldlM step acc := by
  induction l generalizing acc with
  | nil => rfl
  | cons a l ih =>
    rw [List.forIn_cons, h, List.foldlM_cons]
    cases step acc a with
    | error e => rfl
    | ok v => simp only [bind, Except.bind, ih]

theorem process_inactivity_updates_eq (p : Preset) (state : BeaconState)
    (current_epoch previous_epoch : Epoch) (eligible participating : List ValidatorIndex)
    (in_leak : Bool)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (hgenesis : current_epoch ≠ GENESIS_EPOCH)
    (heligible : get_eligible_validator_indices p state = .ok eligible)
    (hprevious : get_previous_epoch p state = .ok previous_epoch)
    (hparticipating : get_unslashed_participating_indices p state TIMELY_TARGET_FLAG_INDEX
      previous_epoch = .ok participating)
    (hleak : is_in_inactivity_leak p state = .ok in_leak) :
    process_inactivity_updates p state =
      (eligible.foldlM (inactivityLoopStep p participating in_leak) state.inactivity_scores).map
        fun inactivity_scores => { state with inactivity_scores } := by
  have hne : (current_epoch == GENESIS_EPOCH) = false := by simpa using hgenesis
  unfold process_inactivity_updates
  simp only [bind, Except.bind, pure, Except.pure, hcurrent, hne, heligible, hprevious,
    hparticipating, hleak, Bool.false_eq_true, if_false]
  rw [forIn_yield_eq_foldlM (step := inactivityLoopStep p participating in_leak)]
  · cases eligible.foldlM (inactivityLoopStep p participating in_leak) state.inactivity_scores <;>
      rfl
  · intro index scores
    simp only [inactivityLoopStep, inactivityScoreStep, bind, Except.bind, pure, Except.pure]
    cases hget : listGet scores index with
    | error e => split <;> rfl
    | ok score =>
      obtain ⟨hlt, _⟩ := listGet_ok hget
      have hset : ∀ a, listSet scores index a = .ok (scores.set index a) :=
        fun a => listSet_of_lt a hlt
      have hget' : ∀ a, listGet (scores.set index a) index = .ok a := fun a => listGet_set a hlt
      have hset' : ∀ a b, listSet (scores.set index a) index b = .ok (scores.set index b) := by
        intro a b
        rw [listSet_of_lt b (by rw [List.length_set]; exact hlt), List.set_set]
      by_cases hp : index ∈ participating
      · cases in_leak <;> simp [hp, hset, hget', hset']
      · cases hadd : uint64Add score p.INACTIVITY_SCORE_BIAS with
        | error e => simp [hp, hadd]
        | ok raised => cases in_leak <;> simp [hp, hadd, hset, hget', hset']

/-- In a leak, a validator that missed the target gains exactly `INACTIVITY_SCORE_BIAS`. -/
theorem inactivityScoreStep_leak_missed (p : Preset) (score : Uint64)
    (hfit : score + p.INACTIVITY_SCORE_BIAS < UINT64_SIZE) :
    inactivityScoreStep p false true score = .ok (score + p.INACTIVITY_SCORE_BIAS) := by
  simp [inactivityScoreStep, uint64Add, hfit, bind, Except.bind, pure, Except.pure]

/-- The score of a validator that hit the target never goes up. -/
theorem inactivityScoreStep_participating_le (p : Preset) (in_leak : Bool) (score new_score : Uint64)
    (h : inactivityScoreStep p true in_leak score = .ok new_score) : new_score ≤ score := by
  simp only [inactivityScoreStep, bind, Except.bind, pure, Except.pure, if_true] at h
  have h2 : ∀ a b, saturating_sub a b ≤ a := by
    intro a b; unfold saturating_sub; split <;> exact Nat.sub_le _ _
  have h1 : saturating_sub score 1 ≤ score := h2 _ _
  cases in_leak
  · simp only [Bool.false_eq_true, if_false, Except.ok.injEq] at h
    rw [← h]; exact Nat.le_trans (h2 _ _) h1
  · simp only [if_true, Except.ok.injEq] at h
    rw [← h]; exact h1

end EpochProofs.Spec
