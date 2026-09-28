# lean_types proofs

Machine-checked proofs that the two `Slot` justification rules in `../src/slot.rs` compute
exactly the leanSpec rules, for every pair of `u64` inputs, and never panic.

`LeanTypesProofs/Generated.lean` is produced mechanically from `../src/slot.rs` by [Charon]
and [Aeneas].

```
../src/slot.rs
   --charon--> pure.llbc --aeneas--> LeanTypesProofs/Generated.lean
                                            |
                                     LeanTypesProofs/Correctness.lean
```

## The theorems

```lean
theorem is_justifiable_after_spec (slot finalized_slot : U64) :
    lean_types.slot.is_justifiable_after slot finalized_slot
      ⦃ b => b = true ↔ IsJustifiableAfter slot.val finalized_slot.val ⦄

theorem justified_index_after_spec (slot finalized_slot : U64) :
    lean_types.slot.justified_index_after slot finalized_slot
      ⦃ r => r.map (·.val) =
        if slot.val ≤ finalized_slot.val ∨ Usize.max < slot.val - finalized_slot.val - 1 then none
        else some (slot.val - finalized_slot.val - 1) ⦄

theorem justified_index_after_spec_64bit (slot finalized_slot : U64)
    (h64 : System.Platform.numBits = 64) :
    lean_types.slot.justified_index_after slot finalized_slot
      ⦃ r => r.map (·.val) = justifiedIndexAfter slot.val finalized_slot.val ⦄
```

`f x ⦃ r => P r ⦄` means that `f x` returns `ok r` and `P r` holds. A panic or an overflow is
a failure in the model, so each theorem also proves that the function does not panic.

`IsJustifiableAfter` and `justifiedIndexAfter` are in `LeanTypesProofs/Spec.lean`. This file
holds nothing else, so you can review the specification on its own:

- A slot is justifiable after a finalized slot when it is not before it, and the distance
  `delta` is at most 5, a perfect square `n * n`, or a pronic number `n * (n + 1)`.
- The justified index is `none` when the slot is at or before the finalized slot. Else it is
  `slot - finalized_slot - 1`.

The general `justified_index_after` theorem also covers a 32-bit `usize`. On that target an
index larger than `usize::MAX` gives `none`. The 64-bit corollary removes that case.

Supporting lemmas:

| Lemma | What it says |
|---|---|
| `square_iff` | if `r` is the integer square root of `d`, then `d` is a square iff `r * r = d` |
| `pronic_iff` | if `s` is the integer square root of `4 * d + 1`, then `d` is pronic iff `s * s = 4 * d + 1` |

## Trusted base

The proof also trusts these items:

- **The `isqrt` axioms.** The Aeneas library has no model of `u64::isqrt` or `u128::isqrt`.
  `Generated.lean` declares both as opaque functions. `LeanTypesProofs/Isqrt.lean` gives each
  one the standard specification, and nothing more: the result `r` satisfies
  `r * r ≤ x < (r + 1) * (r + 1)`. The Rust documentation for `isqrt` states the same property.
- **Charon and Aeneas.** The proof trusts that the Lean they emit models the Rust correctly.
- **The Aeneas Lean library.** The build shows four ``declaration uses `sorry` `` warnings
  from it, two in `Aeneas/Std/Slice.lean` and two in `Aeneas/Std/StringIter.lean`. The
  theorems here do not use them. The axiom audit enforces this, because a use shows up as
  `sorryAx`.
- **mathlib** and the Lean kernel.

## Axiom audit

`LeanTypesProofs/AxiomAudit.lean` walks every theorem in the library. It fails the build if a
theorem depends on an axiom outside this list:

- `propext`, `Classical.choice`, `Quot.sound` (the standard Lean axioms)
- `lean_types.core.num.U64.isqrt`, `lean_types.core.num.U128.isqrt` (the opaque functions
  from `Generated.lean`)
- `LeanTypesProofs.u64_isqrt_spec`, `LeanTypesProofs.u128_isqrt_spec` (the specifications in
  `Isqrt.lean`)

A `sorry` shows up as `sorryAx`, so the audit rejects incomplete proofs too. The audit also
fails if the library declares an axiom that is not in the list. It fails if one of the four
`isqrt` axioms disappears, so the list stays exact. CI runs the audit again directly, because
Lake can replay a cached log instead of elaborating the file again.

## Building

```sh
cd lean/lean_types/proofs
lake exe cache get      # mathlib oleans
lake build
```

## Regenerating after editing `slot.rs`

`Generated.lean` is checked in, so the proof builds without the Rust toolchain. If you change
`is_justifiable_after`, `justified_index_after` or `IMMEDIATE_JUSTIFICATION_WINDOW`, you must
regenerate it. The proof can then need changes too. `Generated.lean` records source line
numbers, so an edit that moves these functions also changes it.

Build Charon and Aeneas once. Charon is pinned in `.github/workflows/lean-types-proofs.yml`.
Aeneas is pinned in `lakefile.toml`. The build steps are in the "Build Charon and Aeneas" step
of the workflow. They work unchanged on a Linux machine.

Then run the script. It runs Charon and Aeneas and overwrites `Generated.lean`:

```sh
CHARON=path/to/charon/charon/target/release/charon \
AENEAS=path/to/aeneas/src/_build/default/main.exe \
  lean/lean_types/proofs/regenerate.sh
```

CI runs the same script. CI fails if the result is different from the checked-in file.

## Why `slot.rs` is written the way it is

Aeneas translates only a subset of Rust. The constraints for this file:

- **Free functions.** `charon --start-from` does not accept methods of an inherent impl. The
  `Slot` methods call the free functions `is_justifiable_after` and `justified_index_after`.
- **No `?` on `Option`.** Aeneas has no model of the `Try` trait for `Option`. The functions
  use `let ... else` instead.
- **No `usize::try_from(u64)`, `Option::ok` or `Option` equality.** Aeneas has no model of
  them. The index function compares against `usize::MAX` and then casts.
- **No `saturating_mul`.** Aeneas has no model of it. The functions use plain `*` and `+`. The
  theorem proves that these operations do not overflow: the root of a `u64` is less than
  `2^32`, and `4 * delta + 1` fits in a `u128`.

[Charon]: https://github.com/AeneasVerif/charon
[Aeneas]: https://github.com/AeneasVerif/aeneas
