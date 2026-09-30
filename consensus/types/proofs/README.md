# Slot duration schedule proofs

Machine-checked proofs for the slot duration schedule. The proofs cover
`compute_time_at_slot_ms` and `compute_slot_at_time_ms` in `../src/core/slot_duration_schedule.rs`.
They also cover `validate_schedule` and `validate_slot_duration_changes` in
`../src/features/eip8198/validation.rs`, the EIP-8198 checks of `SLOT_DURATION_SCHEDULE`.

`SlotScheduleProofs/Generated.lean` is produced mechanically from these two files by [Charon]
and [Aeneas]. The proofs are about this generated model of the real Rust functions, not about a
copy.

```
../src/core/slot_duration_schedule.rs
../src/features/eip8198/validation.rs
   --charon--> pure.llbc --aeneas--> SlotScheduleProofs/Generated.lean
                                            |
                                     SlotScheduleProofs/Correctness.lean
```

## The theorems

All theorems are in `SlotScheduleProofs/Correctness.lean`. `f x ⦃ r => P r ⦄` means that `f x`
returns `ok r` and `P r` holds. A panic is a failure in the model, so each theorem in this form
also proves that the function does not panic.

The slot and time theorems assume `ScheduleOk schedule slots_per_epoch` (see `Model.lean`):

- The last entry of the slice (the oldest) is at epoch 0.
- The epochs strictly decrease along the slice. `EpochSchedule` stores the newest entry first.
- Every slot duration is positive.
- The start slot of every entry fits in a `u64`.
- `slots_per_epoch` is positive.

| Theorem | What it says |
|---|---|
| `slot_at_time_at_slot` | Round trip. If `compute_time_at_slot_ms` returns `Ok(t)` for `slot`, then `compute_slot_at_time_ms` returns `Ok(slot)` for `t`. |
| `time_at_slot_at_time` | Floor. At or after genesis, if `compute_slot_at_time_ms` returns `Ok(slot)` for `t`, then the time of `slot` is `Ok` and at most `t`. The time of `slot + 1` is more than `t`, or it does not fit in a `u64`. |
| `compute_slot_at_time_ms_total` | At or after genesis, `compute_slot_at_time_ms` returns `Ok`. |
| `compute_time_at_slot_ms_err_iff` | `compute_time_at_slot_ms` never panics. It returns `Err` exactly when the time of the slot does not fit in a `u64`. |
| `validate_schedule_ok` | If `validate_schedule` returns `Ok(())` for a non-empty, sorted schedule and `slots_per_epoch` is positive, the schedule meets `ScheduleOk`. The genesis entry has the genesis slot duration. |
| `validate_changes_ok` | If `validate_slot_duration_changes` returns `Ok(())`, every entry is at epoch 0 or at the EIP-8198 fork epoch. |
| `compute_time_at_slot_ms_spec` | `compute_time_at_slot_ms` returns `specTimeAtSlot`, or `Err` when that value does not fit in a `u64`. |
| `compute_slot_at_time_ms_spec` | `compute_slot_at_time_ms` returns `specSlotAtTime`: `Ok` at or after genesis and `Err` before genesis. |

`specTimeAtSlot` and `specSlotAtTime` are in `SlotScheduleProofs/Spec.lean`. They are the direct
definitions from `spec_time_at_slot` and `spec_slot_at_time` in the
`slot_duration_schedule_properties` test module, over natural numbers. `Spec.lean` holds nothing
else, so you can review the specification on its own.

The proofs are not limited to the schedules that `validate_schedule` accepts today. They hold for any
number of slot duration changes that meet `ScheduleOk`.

## Files

| File | Contents |
|---|---|
| `Generated.lean` | The Aeneas model of the Rust. Do not edit it by hand. |
| `SaturatingMul.lean` | The `u64::saturating_mul` specification axiom. |
| `Spec.lean` | The direct definitions. |
| `Walk.lean` | The walk over the schedule, over natural numbers, and its floor and round trip properties. |
| `Model.lean` | `ScheduleOk` and the link from the Rust slice to the walk. |
| `Primitives.lean` | Specifications of the scalar operations and small helpers. |
| `Loops.lean` | The Rust loops compute the walk. |
| `Validate.lean` | What the `validate_schedule` and `validate_slot_duration_changes` loops show. |
| `Correctness.lean` | The theorems above. |
| `Axioms.lean` | The axiom audit. |

## Building

```sh
cd consensus/types/proofs
lake exe cache get      # mathlib oleans
lake build
```

`SlotScheduleProofs/Axioms.lean` is the axiom audit. Its `run_cmd` walks every theorem in the
library. It fails the build if a theorem depends on an axiom outside this list:

- `propext`, `Classical.choice`, `Quot.sound` (the standard Lean axioms)
- `types.core.num.U64.saturating_mul` and `SlotScheduleProofs.u64_saturating_mul_spec`
- the four opaque formatting functions in `Generated.lean`, and `core::fmt::Formatter`
- the `decide +native` axioms that `Generated.lean` declares for its string literals

A `sorry` shows up as `sorryAx`, so the audit rejects incomplete proofs too. The audit also fails
if the library declares an axiom that is not in the list. It fails if a listed axiom disappears,
so the list stays exact.

`.github/workflows/proofs.yml` runs `lake env lean SlotScheduleProofs/Axioms.lean` directly,
because Lake can replay a cached log instead of elaborating the file again. The workflow also
requires every `#print axioms` line in `Axioms.lean` to report exactly `propext`,
`Classical.choice` and `Quot.sound`. The theorems about the Rust functions also depend on the
model axioms, so those lines list only `walk_floor`, `walk_round_trip`, `timeAtSlot_eq_spec` and
`slotAtTime_eq_spec`. The `run_cmd` audit covers all of the theorems.

## Regenerating after editing the verified Rust

`Generated.lean` is checked in, so the proofs build without the Rust toolchain. If you change
the verified functions, `SlotDurationScheduleEntry`, `Epoch::start_slot` or the `Slot`
arithmetic, you must regenerate it. The proofs can then need changes too. `Generated.lean`
records source line numbers, so an edit that moves these functions also changes it.

```sh
# Charon, pinned to the commit Aeneas expects
git clone https://github.com/AeneasVerif/charon && cd charon
git checkout fea3fc68d445181cf4ce094855a43a17192a2b12
cd charon && cargo build --release

# Aeneas (OCaml 5.2; needs domainslib, so 4.x will not work).
# Use the `rev` pinned in lakefile.toml -- that file is the source of truth, and CI reads it.
git clone https://github.com/AeneasVerif/aeneas && cd aeneas
git checkout 453b09f98f2b593c0544a8ad654b77e2a3bc621a
ln -s ../charon charon && cd src && dune build

# Translate. `--start-from` does not accept methods of inherent impls, so the translation
# starts from the free functions that the `SlotDurationSchedule` methods call. `safe_arith` and
# three `Option` methods are translated from source, because the Aeneas library has no model
# of them. `--dest-file` is absolute because rustc runs in the workspace root.
cd consensus/types
out=$(mktemp -d)
charon cargo --preset=aeneas \
  --start-from 'types::core::slot_duration_schedule::compute_time_at_slot_ms' \
  --start-from 'types::core::slot_duration_schedule::compute_slot_at_time_ms' \
  --start-from 'types::features::eip8198::validation::validate_schedule' \
  --start-from 'types::features::eip8198::validation::validate_slot_duration_changes' \
  --include safe_arith --include 'core::option::_::map' --include 'core::option::_::is_some_and' \
  --dest-file "$out/pure.llbc" -- --lib
aeneas -backend lean "$out/pure.llbc" -dest "$out"
cp "$out/Pure.lean" proofs/SlotScheduleProofs/Generated.lean
```

This is the command in `.github/workflows/proofs.yml`, with the `entrypoint` and `charon_extra`
of the `slot-duration-schedule` entry in `.github/proofs.json`. CI fails if the result is
different from the checked-in file.

Charon builds the `types` crate with a standard library from `cargo miri setup`. If another
nightly already wrote that library to `~/.cache/miri`, the build fails with `E0514`. In that
case, build a separate one and give it to Charon:

```sh
MIRI_SYSROOT=$HOME/miri-sysroot cargo +nightly-2026-08-18 miri setup
export CHARON_MIRI_SYSROOTS=$HOME/miri-sysroot
```

## Why the verified Rust is written the way it is

Aeneas translates only a subset of Rust. The constraints for `slot_duration_schedule.rs` and
`validation.rs`:

- **Free functions.** `charon --start-from` does not accept methods of an inherent impl. The
  `SlotDurationSchedule` methods call the free functions `compute_time_at_slot_ms` and
  `compute_slot_at_time_ms`. The feature function `validate_slot_duration_schedule` in
  `features/eip8198/mod.rs` reads the `ChainSpec` and calls `validate_schedule` and
  `validate_slot_duration_changes`.
- **A separate module for the checks.** `validation.rs` holds only the two checks, so the
  translation does not reach `ChainSpec`.
- **Index loops.** Aeneas has no model of `Iterator::rev`, and it cannot translate
  `Iterator::find` or `slice::windows`. The functions walk the slice with `while` loops and
  `get(index)`.
- **No early return from a loop.** Aeneas does not support `return` or `?` inside a loop. An
  overflow inside the time loop sets `overflow` and breaks. The validation loops record the first
  bad entry and break, and the function returns the error after the loop.
- **No borrows out of a loop.** Aeneas cannot keep a reference to an entry after its loop. The
  validation loops copy the epoch and the slot duration instead.
- **No `Option` equality.** Aeneas has no model of it. The fork epoch check uses `is_some_and`.
- **Slots compared as `u64`.** The `PartialOrd` model for `Slot` does not type-check at the pinned
  revision. The time loop compares `slot.as_u64()` with `entry_slot.as_u64()`.

The behaviour is the same as before, including every error value and error string.

## Trusted base

The proofs also trust these items:

- **The `saturating_mul` axiom.** The Aeneas library has no model of `u64::saturating_mul`.
  `Generated.lean` declares it as an opaque function, and `SaturatingMul.lean` gives it the
  standard specification and nothing more: the result is `min(x * y, u64::MAX)`.
  `Epoch::start_slot` calls it.
- **The formatting axioms.** `validate_schedule` and `validate_slot_duration_changes` build their error strings with
  `format!` and `to_string`. `Generated.lean` declares four formatting functions as opaque, and
  the Aeneas library declares `core::fmt::Formatter` as opaque. No axiom states anything about
  them. Aeneas's `toStr` checks the length of each string literal with `decide +native`, which
  adds an auxiliary axiom for each literal.
- **Charon and Aeneas.** The proofs trust that the Lean they emit models the Rust correctly.
  `safe_arith`, `Option::map` and `Option::is_some_and` are translated from their Rust source.
- **The Aeneas Lean library.** The build shows four ``declaration uses `sorry` `` warnings from
  it, two in `Aeneas/Std/Slice.lean` and two in `Aeneas/Std/StringIter.lean`. The theorems here
  do not use them. The axiom audit enforces this, because a use shows up as `sorryAx`.
- **mathlib** and the Lean kernel.

## What the proofs do *not* cover

- `slot_duration_ms_for_epoch`. It calls the generic `EpochSchedule::entry_for_epoch`, which uses
  `Iterator::find` with a closure. Aeneas cannot translate that at the pinned revision.
- `validate_slot_duration_schedule` in `features/eip8198/mod.rs`. It reads `SLOT_DURATION_MS`
  and the EIP-8198 fork epoch from the `ChainSpec` and passes them to the two checks.
- That the slice is sorted. `EpochSchedule::new` sorts it, and `validate_schedule_ok` takes the
  order as a hypothesis. The proof does not cover `sort_by_key`.
- The error strings of `validate_schedule` and `validate_slot_duration_changes`. The theorems only use their `Ok(())`
  results.

[Charon]: https://github.com/AeneasVerif/charon
[Aeneas]: https://github.com/AeneasVerif/aeneas
