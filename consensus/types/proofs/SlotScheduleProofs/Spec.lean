/-!
# The slot duration schedule

The time of a slot and the slot at a time, stated over natural numbers. These are the direct
definitions from `spec_time_at_slot` and `spec_slot_at_time` in the
`slot_duration_schedule_properties` test module in `slot_duration_schedule.rs`. Nothing here
mentions the Rust or its generated model, so you can review the specification on its own.

`schedule` lists `(epoch, slot duration in ms)` pairs, oldest first. The first entry is the
genesis entry at epoch 0.
-/

namespace SlotScheduleProofs

/-- The time of `slot`. Walk the schedule from the newest entry to the oldest. Each entry that
    starts before the current end slot adds the slots from its start to the end slot, at its
    slot duration. -/
def specTimeAtSlot (slotsPerEpoch : Nat) (schedule : List (Nat × Nat)) (genesisTime slot : Nat) :
    Nat :=
  (schedule.reverse.foldl
    (fun (acc : Nat × Nat) (entry : Nat × Nat) =>
      if entry.1 * slotsPerEpoch < acc.2 then
        (acc.1 + (acc.2 - entry.1 * slotsPerEpoch) * entry.2, entry.1 * slotsPerEpoch)
      else acc)
    (genesisTime, slot)).1

/-- The slot at `time`. Find the newest entry that starts at or before `time`, and count the
    whole slots of that entry from its start to `time`. There is no slot before genesis. -/
def specSlotAtTime (slotsPerEpoch : Nat) (schedule : List (Nat × Nat)) (genesisTime time : Nat) :
    Option Nat :=
  schedule.reverse.findSome? fun entry =>
    let entryTime := specTimeAtSlot slotsPerEpoch schedule genesisTime (entry.1 * slotsPerEpoch)
    if entryTime ≤ time then
      some (entry.1 * slotsPerEpoch + (time - entryTime) / entry.2)
    else none

end SlotScheduleProofs
