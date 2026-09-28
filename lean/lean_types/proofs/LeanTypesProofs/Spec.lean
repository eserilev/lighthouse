/-!
# The justification rules

The two `Slot` rules from leanSpec (`src/lean_spec/spec/forks/lstar/slot.py`), stated over
natural numbers. Nothing here mentions the Rust or its generated model. This file is the
specification the theorems in `Correctness.lean` are checked against, so it is kept on its
own and short.
-/

namespace LeanTypesProofs

/-- A slot is justifiable after `finalizedSlot` when it is not before it, and its distance
    `delta` from it is at most 5, a perfect square, or a pronic number `n * (n + 1)`. -/
def IsJustifiableAfter (slot finalizedSlot : Nat) : Prop :=
  finalizedSlot ≤ slot
  ∧ (slot - finalizedSlot ≤ 5
    ∨ (∃ n, slot - finalizedSlot = n * n)
    ∨ (∃ n, slot - finalizedSlot = n * (n + 1)))

/-- The bitfield index of a slot after `finalizedSlot`. Slots at or before `finalizedSlot` have
    no index. Slot `finalizedSlot + 1` has index 0. -/
def justifiedIndexAfter (slot finalizedSlot : Nat) : Option Nat :=
  if slot ≤ finalizedSlot then none else some (slot - finalizedSlot - 1)

end LeanTypesProofs
