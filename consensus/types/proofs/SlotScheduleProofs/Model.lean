import SlotScheduleProofs.Generated
import SlotScheduleProofs.Walk

/-!
# The schedule in the generated model

Definitions that connect the Rust slice of schedule entries to the walk over natural numbers in
`Walk.lean`, and the preconditions `ScheduleOk` that the theorems assume.
-/

namespace SlotScheduleProofs

open Aeneas Aeneas.Std Aeneas.Std.WP Result types

/-- A schedule entry in the generated model. -/
abbrev Entry := core.slot_duration_schedule.SlotDurationScheduleEntry

/-- The start slot and the slot duration of each entry, newest first, over natural numbers. The
    start slot is the exact product `epoch * slots_per_epoch`. -/
def entries (schedule : Slice Entry) (spe : U64) : List (Nat × Nat) :=
  schedule.val.map fun e => (e.epoch.val * spe.val, e.slot_duration_ms.val)

/-- The schedule as `(epoch, slot duration)` pairs, oldest first, as `Spec.lean` takes it. -/
def schedulePairs (schedule : Slice Entry) : List (Nat × Nat) :=
  (schedule.val.map fun e => (e.epoch.val, e.slot_duration_ms.val)).reverse

/-- The start slot of every entry fits in a `u64`, so `Epoch::start_slot` does not saturate. -/
def StartSlotsFit (schedule : Slice Entry) (spe : U64) : Prop :=
  ∀ e ∈ schedule.val, e.epoch.val * spe.val ≤ U64.max

/-- The preconditions of the time and slot theorems. The slice is newest first, as
    `EpochSchedule` stores it.

    - `genesis`: the last (oldest) entry is at epoch 0. This also makes the slice non-empty.
    - `decreasing`: the epochs strictly decrease along the slice.
    - `positive`: every slot duration is positive.
    - `fits`: the start slot of every entry fits in a `u64`.
    - `slots_per_epoch_pos`: `slots_per_epoch` is positive.

    `validate_schedule_ok` in `Correctness.lean` derives all of them from `validate_schedule`. -/
structure ScheduleOk (schedule : Slice Entry) (spe : U64) : Prop where
  genesis : schedule.val.getLast?.map (·.epoch.val) = some 0
  decreasing : ∀ j (h : j + 1 < schedule.length),
    schedule.val[j + 1].epoch.val < (schedule.val[j]'(Nat.lt_of_succ_lt h)).epoch.val
  positive : ∀ e ∈ schedule.val, 0 < e.slot_duration_ms.val
  fits : StartSlotsFit schedule spe
  slots_per_epoch_pos : 0 < spe.val

theorem entryAt_entries {schedule : Slice Entry} {spe : U64} {i : Nat}
    (h : i < schedule.length) :
    entryAt (entries schedule spe) i =
      (schedule.val[i].epoch.val * spe.val, schedule.val[i].slot_duration_ms.val) := by
  simp [entryAt, entries, List.getD_eq_getElem?_getD, h]

theorem entries_eq (schedule : Slice Entry) (spe : U64) :
    entries schedule spe = (schedulePairs schedule).reverse.map fun p => (p.1 * spe.val, p.2) := by
  simp [entries, schedulePairs, Function.comp_def]

theorem ScheduleOk.length_pos {schedule : Slice Entry} {spe : U64} (h : ScheduleOk schedule spe) :
    0 < schedule.length := by
  have := h.genesis
  cases hs : schedule.val with
  | nil => simp [hs] at this
  | cons _ _ => simp [Slice.length, hs]

theorem ScheduleOk.chain {schedule : Slice Entry} {spe : U64} (h : ScheduleOk schedule spe) :
    ∀ i, i < schedule.length →
      Chain (entries schedule spe) i (entryAt (entries schedule spe) i).1
        (entryAt (entries schedule spe) i).2
  | 0, hi => by
    rw [entryAt_entries hi]
    exact h.positive _ (List.getElem_mem _)
  | i + 1, hi => by
    rw [entryAt_entries hi]
    refine ⟨h.positive _ (List.getElem_mem _), by simp [entries, Slice.length] at hi ⊢; omega, ?_,
      h.chain i (by omega)⟩
    rw [entryAt_entries (by omega)]
    exact Nat.mul_lt_mul_of_pos_right (h.decreasing i hi) h.slots_per_epoch_pos

theorem ScheduleOk.genesis_slot {schedule : Slice Entry} {spe : U64} (h : ScheduleOk schedule spe) :
    (entryAt (entries schedule spe) (schedule.length - 1)).1 = 0 := by
  have hpos := h.length_pos
  rw [entryAt_entries (by omega)]
  have := h.genesis
  rw [List.getLast?_eq_getElem?, List.getElem?_eq_getElem (by simp [Slice.length] at hpos ⊢; omega)] at this
  simp only [Option.map_some, Option.some.injEq] at this
  simp [Slice.length, this]

theorem ScheduleOk.chain_top {schedule : Slice Entry} {spe : U64} (h : ScheduleOk schedule spe) :
    Chain (entries schedule spe) (schedule.length - 1) 0
      (entryAt (entries schedule spe) (schedule.length - 1)).2 := by
  have := h.chain (schedule.length - 1) (by have := h.length_pos; omega)
  rwa [h.genesis_slot] at this

theorem ScheduleOk.entries_split {schedule : Slice Entry} {spe : U64} (h : ScheduleOk schedule spe) :
    entries schedule spe = (entries schedule spe).take (schedule.length - 1) ++
      [(0, (entryAt (entries schedule spe) (schedule.length - 1)).2)] := by
  have hpos := h.length_pos
  have hlen : (entries schedule spe).length = schedule.length := by simp [entries, Slice.length]
  have := take_succ_entryAt (es := entries schedule spe) (i := schedule.length - 1) (by omega)
  rw [show schedule.length - 1 + 1 = (entries schedule spe).length by omega, List.take_length] at this
  conv_lhs => rw [this]
  rw [← h.genesis_slot]

/-- The time of `slot`, as the walk over natural numbers computes it without overflow. -/
def timeAtSlot (schedule : Slice Entry) (spe genesis_time_ms : U64) (slot : Nat) : Nat :=
  walkTime (entries schedule spe) slot (schedule.length - 1) 0 genesis_time_ms.val
    (entryAt (entries schedule spe) (schedule.length - 1)).2

/-- The slot at `time`, as the walk over natural numbers computes it without overflow. -/
def slotAtTime (schedule : Slice Entry) (spe genesis_time_ms : U64) (time : Nat) : Nat :=
  walkSlot (entries schedule spe) time (schedule.length - 1) 0 genesis_time_ms.val
    (entryAt (entries schedule spe) (schedule.length - 1)).2

end SlotScheduleProofs
