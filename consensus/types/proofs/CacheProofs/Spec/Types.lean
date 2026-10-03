/-!
# Spec types and `Uint64` arithmetic

Transcribed from the consensus specs (v1.7.0-beta.2). Names match the spec.

pyspec raises on `Uint64` overflow and on division by zero. Here a raise is `Except.error`.

Write the `Spec` files from the spec only. Do not read Lighthouse.
-/

namespace CacheProofs.Spec

abbrev Gwei := Nat
abbrev Uint64 := Nat
abbrev Epoch := Nat

inductive SpecError where
  | overflow
  | divisionByZero
  | indexOutOfRange
  | assertionFailed
  deriving DecidableEq, Repr

abbrev SpecM := Except SpecError

def UINT64_SIZE : Nat := 2 ^ 64

def FAR_FUTURE_EPOCH : Epoch := 2 ^ 64 - 1

def uint64Add (a b : Uint64) : SpecM Uint64 :=
  if a + b < UINT64_SIZE then pure (a + b) else throw .overflow

def uint64Sub (a b : Uint64) : SpecM Uint64 :=
  if b ≤ a then pure (a - b) else throw .overflow

end CacheProofs.Spec
