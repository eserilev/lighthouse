import EpochProofs.Spec.Types

/-!
# Separate passes and one pass over validator rows

The spec runs each epoch step as its own pass over the validators. Lighthouse runs all steps
for one validator, then moves to the next validator. `two_passes_eq_one_pass` shows that the two orders give
the same result, if each step reads only its own row and its own accumulator.

The two orders can fail at different places. For example, pass 1 can fail on row 5 while the
single pass fails first on row 2 in step 2. So the theorems state `SameOk`: the same `ok` values,
and so an error in one order exactly when there is an error in the other order.
-/

namespace EpochProofs.Spec

/-- One pass over the rows. The step reads and writes one row and an accumulator. -/
def passM {A R : Type} (f : A → R → SpecM (A × R)) : A → List R → SpecM (A × List R)
  | a, [] => pure (a, [])
  | a, r :: rs => do
    let x ← f a r
    let y ← passM f x.1 rs
    pure (y.1, x.2 :: y.2)

/-- Step `f`, then step `g`, on one row. -/
def bothSteps {A B R : Type} (f : A → R → SpecM (A × R)) (g : B → R → SpecM (B × R)) :
    A × B → R → SpecM ((A × B) × R) := fun ab r => do
  let x ← f ab.1 r
  let y ← g ab.2 x.2
  pure ((x.1, y.1), y.2)

/-- Pass `f` over all rows, then pass `g` over all rows. -/
def twoPasses {A B R : Type} (f : A → R → SpecM (A × R)) (g : B → R → SpecM (B × R))
    (a : A) (b : B) (rs : List R) : SpecM ((A × B) × List R) := do
  let x ← passM f a rs
  let y ← passM g b x.2
  pure ((x.1, y.1), y.2)

/-- The two computations give the same `ok` values. -/
def SameOk {α : Type} (x y : SpecM α) : Prop := ∀ v, x = .ok v ↔ y = .ok v

theorem SameOk.refl {α : Type} (x : SpecM α) : SameOk x x := fun _ => Iff.rfl

theorem SameOk.trans {α : Type} {x y z : SpecM α} (h1 : SameOk x y) (h2 : SameOk y z) :
    SameOk x z := fun v => (h1 v).trans (h2 v)

theorem SameOk.of_eq {α : Type} {x y : SpecM α} (h : x = y) : SameOk x y := by
  subst h; exact SameOk.refl x

theorem SameOk.errors {α : Type} {e1 e2 : SpecError} :
    SameOk (Except.error e1 : SpecM α) (Except.error e2) := fun _ => by simp

theorem SameOk.map {α β : Type} {x y : SpecM α} (h : SameOk x y) (k : α → β) :
    SameOk (k <$> x) (k <$> y) := by
  intro w
  cases x with
  | error e1 =>
    cases y with
    | error e2 => simp [Functor.map, Except.map]
    | ok v2 => exact absurd ((h v2).mpr rfl) (by simp)
  | ok v1 =>
    have hy := (h v1).mp rfl
    subst hy
    exact Iff.rfl

theorem passM_cons {A R : Type} (f : A → R → SpecM (A × R)) (a : A) (r : R) (rs : List R) :
    passM f a (r :: rs) = (do
      let x ← f a r
      let y ← passM f x.1 rs
      pure (y.1, x.2 :: y.2)) := rfl

theorem two_passes_eq_one_pass {A B R : Type} (f : A → R → SpecM (A × R)) (g : B → R → SpecM (B × R))
    (a : A) (b : B) (rs : List R) :
    SameOk (twoPasses f g a b rs) (passM (bothSteps f g) (a, b) rs) := by
  induction rs generalizing a b with
  | nil => exact SameOk.refl _
  | cons r rs ih =>
    simp only [twoPasses, passM_cons, bothSteps]
    cases hf : f a r with
    | error e => exact SameOk.errors
    | ok x =>
    simp only [bind, Except.bind]
    cases hg : g b x.2 with
    | error e =>
      cases passM f x.1 rs with
      | error e' => exact SameOk.errors
      | ok y =>
        simp only [pure, Except.pure]
        simp only [passM_cons, bind, Except.bind, hg]
        exact SameOk.errors
    | ok z =>
    have h := (ih x.1 z.1).map (fun w => ((w.1.1, w.1.2), z.2 :: w.2))
    simp only [twoPasses] at h
    refine SameOk.trans ?_ (SameOk.trans h ?_)
    · apply SameOk.of_eq
      cases passM f x.1 rs with
      | error e => rfl
      | ok y =>
        simp only [passM_cons, bind, Except.bind, hg, pure, Except.pure]
        cases passM g z.1 y.2 <;> rfl
    · apply SameOk.of_eq
      simp only [pure, Except.pure]
      cases passM (bothSteps f g) (x.1, z.1) rs <;> rfl

end EpochProofs.Spec
