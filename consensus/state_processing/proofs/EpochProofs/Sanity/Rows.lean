import EpochProofs.Sanity.SinglePass

/-!
# Validator rows

A row holds what the per-validator epoch steps read and write for one validator. `rowsOf` cuts
a state into rows, and `withRows` writes rows back. Each per-validator pass of the spec is a
`passM` over these rows.
-/

namespace EpochProofs.Spec

structure Row where
  validator : Validator
  balance : Gwei
  inactivity_score : Uint64
  previous_participation : ParticipationFlags
  current_participation : ParticipationFlags
  index : Nat
  deriving DecidableEq, Repr

/-- The per-validator lists of the state have the same length. SSZ states always do. -/
def RowsOk (state : BeaconState) : Prop :=
  state.balances.length = state.validators.length
  ∧ state.inactivity_scores.length = state.validators.length
  ∧ state.previous_epoch_participation.length = state.validators.length
  ∧ state.current_epoch_participation.length = state.validators.length

def rowsOf (state : BeaconState) : List Row :=
  state.validators.zipIdx.map fun (v, i) =>
    { validator := v
      balance := state.balances.getD i 0
      inactivity_score := state.inactivity_scores.getD i 0
      previous_participation := state.previous_epoch_participation.getD i 0
      current_participation := state.current_epoch_participation.getD i 0
      index := i }

/-- Write back the fields that the per-validator steps change. -/
def BeaconState.withRows (state : BeaconState) (rows : List Row) : BeaconState :=
  { state with
    validators := rows.map (·.validator)
    balances := rows.map (·.balance)
    inactivity_scores := rows.map (·.inactivity_score) }

/-- `SameOk` goes through `bind`. -/
theorem SameOk.bind {α β : Type} {x y : SpecM α} {k l : α → SpecM β} (h : SameOk x y)
    (hk : ∀ v, x = .ok v → SameOk (k v) (l v)) : SameOk (x >>= k) (y >>= l) := by
  intro w
  cases hx : x with
  | error e1 =>
    cases hy : y with
    | error e2 => exact ⟨(fun h => nomatch h), (fun h => nomatch h)⟩
    | ok v2 => exact absurd ((h v2).mpr hy) (by simp [hx])
  | ok v1 =>
    have hy := (h v1).mp hx
    rw [hy]
    exact hk v1 hx w

theorem rowsOf_length (state : BeaconState) : (rowsOf state).length = state.validators.length := by
  simp [rowsOf]

theorem rowsOf_getElem (state : BeaconState) (i : Nat) (h : i < (rowsOf state).length) :
    (rowsOf state)[i] =
      { validator := state.validators[i]'(by simpa [rowsOf_length] using h)
        balance := state.balances.getD i 0
        inactivity_score := state.inactivity_scores.getD i 0
        previous_participation := state.previous_epoch_participation.getD i 0
        current_participation := state.current_epoch_participation.getD i 0
        index := i } := by
  simp [rowsOf]

end EpochProofs.Spec
