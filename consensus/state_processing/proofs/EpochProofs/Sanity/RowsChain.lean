import EpochProofs.Sanity.RowsSlashings

/-!
# Chaining row passes

After a pass, the state holds the new rows. `rowsOf_withRows` shows that the next pass reads
back the same rows, if the pass kept the participation flags and the index of each row.
-/

namespace EpochProofs.Spec

/-- The row fields that no per-validator step changes. -/
def rowKey (r : Row) : ParticipationFlags × ParticipationFlags × Nat :=
  (r.previous_participation, r.current_participation, r.index)

theorem rowsOf_withRows (state : BeaconState) (rows : List Row)
    (hkey : rows.map rowKey = (rowsOf state).map rowKey) :
    rowsOf (state.withRows rows) = rows := by
  have hlen : rows.length = state.validators.length := by
    rw [← rowsOf_length, ← List.length_map (f := rowKey), hkey, List.length_map]
  apply List.ext_getElem
  · simp [rowsOf_length, BeaconState.withRows]
  · intro i h1 h2
    have hs : i < state.validators.length := by rw [← hlen]; exact h2
    have hk := congrArg (fun l => l[i]?) hkey
    simp only [List.getElem?_map, List.getElem?_eq_getElem h2,
      List.getElem?_eq_getElem (show i < (rowsOf state).length by rw [rowsOf_length]; exact hs),
      Option.map_some, Option.some.injEq, rowKey, rowsOf_getElem, Prod.mk.injEq] at hk
    obtain ⟨hprev, hcur, hidx⟩ := hk
    rw [rowsOf_getElem]
    simp only [BeaconState.withRows, List.getElem_map, List.getD_eq_getElem?_getD,
      List.getElem?_map, List.getElem?_eq_getElem h2, Option.map_some, Option.getD_some]
    have hp : state.previous_epoch_participation.getD i 0 = rows[i].previous_participation :=
      hprev.symm
    have hc : state.current_epoch_participation.getD i 0 = rows[i].current_participation :=
      hcur.symm
    simp only [List.getD_eq_getElem?_getD] at hp hc
    rw [hp, hc]
    cases h : rows[i]
    simp only [h] at hidx
    simp [hidx]

theorem rowsOk_withRows (state : BeaconState) (rows : List Row) (hrows : RowsOk state)
    (hlen : rows.length = state.validators.length) : RowsOk (state.withRows rows) := by
  obtain ⟨_, _, hp, hc⟩ := hrows
  refine ⟨?_, ?_, ?_, ?_⟩ <;> simp [BeaconState.withRows, hlen, hp, hc]

end EpochProofs.Spec
