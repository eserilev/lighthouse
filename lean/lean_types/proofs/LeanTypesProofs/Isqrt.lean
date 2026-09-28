import LeanTypesProofs.Generated

/-!
# The `isqrt` axioms

The Aeneas library has no model of `u64::isqrt` or `u128::isqrt`, so `Generated.lean` declares
both as opaque functions. This file states the standard integer square root specification for
each, and nothing more: the result `r` satisfies `r * r ≤ x < (r + 1) * (r + 1)`. Rust documents
`isqrt` as the floor of the square root, which is exactly this, and it never panics for
unsigned types.

These are the only axioms the proofs add. `AxiomAudit.lean` pins the list.
-/

namespace LeanTypesProofs

open Aeneas Aeneas.Std Aeneas.Std.WP

@[step]
axiom u64_isqrt_spec (x : U64) :
    lean_types.core.num.U64.isqrt x
      ⦃ r => r.val * r.val ≤ x.val ∧ x.val < (r.val + 1) * (r.val + 1) ⦄

@[step]
axiom u128_isqrt_spec (x : U128) :
    lean_types.core.num.U128.isqrt x
      ⦃ r => r.val * r.val ≤ x.val ∧ x.val < (r.val + 1) * (r.val + 1) ⦄

end LeanTypesProofs
