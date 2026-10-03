import EpochProofs.Spec.PendingConsolidations
import EpochProofs.Sanity.PendingDeposits

/-!
# Sanity theorems for the `process_pending_consolidations` reference

`consolidationStep` is the loop body of `process_pending_consolidations` on a local table of the
validators that consolidations reference: their balances, and for each one whether it exists,
whether it is slashed, its withdrawable epoch and its effective balance. A consolidation is a
pair of positions in that table.

`stepLoop` runs a step until it stops or the list runs out. Its lemmas split the loop at one
position, for the Lighthouse loop proof.
-/

namespace EpochProofs.Spec

/-- A loop that stops early. The step returns the new state and whether to stop. -/
def stepLoop {S X : Type} (step : S → X → SpecM (S × Bool)) : S → List X → SpecM (S × Bool)
  | s, [] => pure (s, false)
  | s, x :: xs => do
    let r ← step s x
    if r.2 then pure (r.1, true) else stepLoop step r.1 xs

theorem stepLoop_append {S X : Type} (step : S → X → SpecM (S × Bool)) (s : S)
    (l1 l2 : List X) :
    stepLoop step s (l1 ++ l2) = (do
      let r ← stepLoop step s l1
      if r.2 then pure r else stepLoop step r.1 l2) := by
  induction l1 generalizing s with
  | nil => simp [stepLoop, pure, Except.pure, bind, Except.bind]
  | cons x xs ih =>
    simp only [List.cons_append, stepLoop, bind, Except.bind]
    cases step s x with
    | error e => rfl
    | ok r =>
      simp only
      by_cases h : r.2 = true
      · simp [h, pure, Except.pure]
      · simp only [h, Bool.false_eq_true, if_false]
        rw [ih]
        rfl

theorem stepLoop_split {S X : Type} (step : S → X → SpecM (S × Bool)) (init st : S)
    (l : List X) (i : Nat) (x : X) (hget : l[i]? = some x)
    (hst : stepLoop step init (l.take i) = .ok (st, false)) :
    stepLoop step init l = (do
      let r ← step st x
      if r.2 then pure (r.1, true) else stepLoop step r.1 (l.drop (i + 1))) := by
  have hlt : i < l.length := (List.getElem?_eq_some_iff.mp hget).1
  have hx : l[i] = x := (List.getElem?_eq_some_iff.mp hget).2
  have hsplit : l = l.take i ++ (x :: l.drop (i + 1)) := by
    rw [← hx, ← List.drop_eq_getElem_cons hlt, List.take_append_drop]
  conv => lhs; rw [hsplit]
  rw [stepLoop_append, hst]
  simp only [bind, Except.bind, Bool.false_eq_true, if_false, stepLoop]

theorem stepLoop_stop_at {S X : Type} (step : S → X → SpecM (S × Bool)) (init st : S)
    (l : List X) (i : Nat) (x : X) (hget : l[i]? = some x)
    (hst : stepLoop step init (l.take i) = .ok (st, false)) (st' : S)
    (hr : step st x = .ok (st', true)) :
    stepLoop step init l = .ok (st', true) := by
  rw [stepLoop_split step init st l i x hget hst, hr]
  rfl

theorem stepLoop_error_at {S X : Type} (step : S → X → SpecM (S × Bool)) (init st : S)
    (l : List X) (i : Nat) (x : X) (hget : l[i]? = some x)
    (hst : stepLoop step init (l.take i) = .ok (st, false)) (e : SpecError)
    (hr : step st x = .error e) :
    stepLoop step init l = .error e := by
  rw [stepLoop_split step init st l i x hget hst, hr]
  rfl

theorem stepLoop_next_at {S X : Type} (step : S → X → SpecM (S × Bool)) (init st : S)
    (l : List X) (i : Nat) (x : X) (hget : l[i]? = some x)
    (hst : stepLoop step init (l.take i) = .ok (st, false)) (st' : S)
    (hr : step st x = .ok (st', false)) :
    stepLoop step init (l.take (i + 1)) = .ok (st', false) := by
  have hlt : i < l.length := (List.getElem?_eq_some_iff.mp hget).1
  have hx : l[i] = x := (List.getElem?_eq_some_iff.mp hget).2
  rw [List.take_add_one, List.getElem?_eq_getElem hlt, hx, Option.toList_some,
    stepLoop_append, hst]
  simp [bind, Except.bind, stepLoop, hr, pure, Except.pure]

theorem stepLoop_end {S X : Type} (step : S → X → SpecM (S × Bool)) (init : S)
    (l : List X) (i : Nat) (hi : l.length ≤ i) (x : S × Bool)
    (hst : stepLoop step init (l.take i) = .ok x) :
    stepLoop step init l = .ok x := by
  rw [List.take_of_length_le hi] at hst
  exact hst

/-- What the moves read from a validator in the local table. -/
structure LocalValidator where
  exists_ : Bool
  slashed : Bool
  withdrawable_epoch : Epoch
  effective_balance : Gwei
  deriving DecidableEq, Repr

def localExists (validators : List LocalValidator) (i : Nat) : Bool :=
  match validators[i]? with
  | some v => v.exists_
  | none => false

/-- The loop body of `process_pending_consolidations` on the local table. The state is the
local balances and `next_pending_consolidation`. -/
def consolidationStep (validators : List LocalValidator) (next_epoch : Nat)
    (s : List Gwei × Nat) (c : Nat × Nat) : SpecM ((List Gwei × Nat) × Bool) := do
  if !localExists validators c.1 then
    throw .indexOutOfRange
  let source_validator ← listGet validators c.1
  if source_validator.slashed then
    return ((s.1, s.2 + 1), false)
  if source_validator.withdrawable_epoch > next_epoch then
    return (s, true)
  let source_balance ← listGet s.1 c.1
  let source_effective_balance := min source_balance source_validator.effective_balance
  let balances ← decrease_balance s.1 c.1 source_effective_balance
  if !localExists validators c.2 then
    throw .indexOutOfRange
  let balances ← increase_balance balances c.2 source_effective_balance
  pure ((balances, s.2 + 1), false)

/-- A `for` loop with `break` is a `stepLoop`, if each body run is one step. `P` holds on each
state that the loop reaches. -/
theorem forIn_eq_stepLoop {S T X : Type} (abs : S → T) (P : S → Prop)
    (body : X → T → SpecM (ForInStep T)) (step : S → X → SpecM (S × Bool))
    (h : ∀ x s, P s → body x (abs s) =
      (step s x).map fun r => if r.2 then ForInStep.done (abs r.1) else ForInStep.yield (abs r.1))
    (hP : ∀ x s r, P s → step s x = .ok r → P r.1)
    (l : List X) (s : S) (hs : P s) :
    forIn l (abs s) body = (stepLoop step s l).map (abs ·.1) := by
  induction l generalizing s with
  | nil => simp [stepLoop, forIn, ForIn.forIn, pure, Except.pure, Except.map]
  | cons x xs ih =>
    rw [List.forIn_cons, h x s hs, stepLoop]
    cases hr : step s x with
    | error e => simp [Except.map, bind, Except.bind]
    | ok r =>
      have hPr := hP x s r hs hr
      by_cases hb : r.2 = true
      · simp [Except.map, bind, Except.bind, hb, pure, Except.pure]
      · simp only [Except.map, bind, Except.bind, hb, Bool.false_eq_true, if_false]
        exact ih r.1 hPr

theorem stepLoop_map {S X Y : Type} (step : S → X → SpecM (S × Bool)) (f : Y → X) (s : S)
    (l : List Y) :
    stepLoop step s (l.map f) = stepLoop (fun s y => step s (f y)) s l := by
  induction l generalizing s with
  | nil => rfl
  | cons y ys ih =>
    simp only [List.map_cons, stepLoop, ih]

/-- A validator of the full registry, as a row of the local table. -/
def globalLocal (v : Validator) : LocalValidator :=
  ⟨true, v.slashed, v.withdrawable_epoch, v.effective_balance⟩

theorem localExists_global (validators : List Validator) (i : Nat) :
    localExists (validators.map globalLocal) i = decide (i < validators.length) := by
  unfold localExists
  rw [List.getElem?_map]
  by_cases h : i < validators.length
  · simp [h, globalLocal]
  · simp [h]

/-- On the full registry, the reference equals `stepLoop consolidationStep`, then a drop of the
processed consolidations. -/
theorem process_pending_consolidations_eq (p : Preset) (state : BeaconState)
    (hlen : state.balances.length = state.validators.length) :
    process_pending_consolidations p state = (do
      let next_epoch ← uint64Add (← get_current_epoch p state) 1
      let r ← stepLoop (consolidationStep (state.validators.map globalLocal) next_epoch)
        (state.balances, 0)
        (state.pending_consolidations.map fun c => (c.source_index, c.target_index))
      pure { state with
        balances := r.1.1
        pending_consolidations := state.pending_consolidations.drop r.1.2 }) := by
  unfold process_pending_consolidations
  simp only [bind, Except.bind, pure, Except.pure]
  cases get_current_epoch p state with
  | error e => rfl
  | ok epoch =>
  simp only
  cases uint64Add epoch 1 with
  | error e => rfl
  | ok next_epoch =>
  simp only
  have hinit : (⟨0, state⟩ : MProd Nat BeaconState) =
      (fun s : List Gwei × Nat => (⟨s.2, { state with balances := s.1 }⟩ : MProd Nat BeaconState))
        (state.balances, 0) := by
    cases state; rfl
  rw [hinit, forIn_eq_stepLoop
    (fun s : List Gwei × Nat => (⟨s.2, { state with balances := s.1 }⟩ : MProd Nat BeaconState))
    (fun s => s.1.length = state.validators.length) _
    (fun s c => consolidationStep (state.validators.map globalLocal) next_epoch s
      (c.source_index, c.target_index)), stepLoop_map]
  · cases stepLoop (fun s c => consolidationStep (state.validators.map globalLocal) next_epoch s
      (c.source_index, c.target_index)) (state.balances, 0) state.pending_consolidations <;> rfl
  · rintro ⟨src, tgt⟩ ⟨bal, n⟩ hbs
    dsimp only at hbs ⊢
    by_cases hsrc : src < state.validators.length
    case neg =>
      simp [consolidationStep, localExists_global, hsrc, listGet,
          Except.map, bind, Except.bind, 
          throw, throwThe, MonadExceptOf.throw]
    have hsrcb : src < bal.length := by rw [hbs]; exact hsrc
    by_cases hsl : state.validators[src].slashed = true
    · simp [consolidationStep, localExists_global, hsrc, listGet, 
        hsl, globalLocal, Except.map, bind, Except.bind, pure, Except.pure]
    by_cases hwe : state.validators[src].withdrawable_epoch > next_epoch
    · simp [consolidationStep, localExists_global, hsrc, listGet, 
        hsl, hwe, globalLocal, Except.map, bind, Except.bind, pure, Except.pure]
    by_cases htgt : tgt < state.validators.length
    case neg =>
      have htgtb : ¬ tgt < bal.length := by rw [hbs]; exact htgt
      simp [consolidationStep, localExists_global, hsrc, htgt, listGet, listSet,
        hsrcb, hsl, hwe,
        globalLocal, decrease_balance, increase_balance,
        htgtb,
        Except.map, bind, Except.bind, pure, Except.pure, throw, throwThe, MonadExceptOf.throw]
    have htgtb : tgt < bal.length := by rw [hbs]; exact htgt
    simp [consolidationStep, localExists_global, hsrc, htgt, listGet, listSet,
      hsrcb, hsl, hwe,
      globalLocal, decrease_balance, increase_balance, htgtb,
      
      Except.map, bind, Except.bind, pure, Except.pure]
    generalize uint64Add _ _ = x
    cases x <;> rfl
  · rintro ⟨src, tgt⟩ ⟨bal, n⟩ r hbs hr
    dsimp only at hbs ⊢
    rw [← hbs]
    have hsrcb : src < state.validators.length → src < bal.length := by rw [hbs]; exact id
    have htgtb : tgt < state.validators.length → tgt < bal.length := by rw [hbs]; exact id
    by_cases hsrc : src < state.validators.length
    case neg =>
      simp [consolidationStep, localExists_global, hsrc, bind, Except.bind, throw, throwThe,
        MonadExceptOf.throw] at hr
    by_cases hsl : state.validators[src].slashed = true
    · simp [consolidationStep, localExists_global, hsrc, listGet, hsl, globalLocal, bind,
        Except.bind, pure, Except.pure] at hr
      subst hr; rfl
    by_cases hwe : state.validators[src].withdrawable_epoch > next_epoch
    · simp [consolidationStep, localExists_global, hsrc, listGet, hsl, hwe, globalLocal, bind,
        Except.bind, pure, Except.pure] at hr
      subst hr; rfl
    by_cases htgt : tgt < state.validators.length
    case neg =>
      simp [consolidationStep, localExists_global, hsrc, htgt, listGet, listSet, hsrcb hsrc, hsl,
        hwe, globalLocal, decrease_balance, bind, Except.bind, pure, Except.pure, throw, throwThe,
        MonadExceptOf.throw] at hr
    simp [consolidationStep, localExists_global, hsrc, htgt, listGet, listSet, hsrcb hsrc, hsl,
      hwe, globalLocal, decrease_balance, increase_balance, htgtb htgt, 
      bind, Except.bind, pure, Except.pure] at hr
    revert hr
    generalize uint64Add _ _ = x
    cases x
    · simp
    · intro hr
      simp at hr
      subst hr
      simp
  · exact hlen

end EpochProofs.Spec
