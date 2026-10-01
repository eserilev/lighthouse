import SlotScheduleProofs.Generated

/-!
# The `saturating_mul` axiom

The Aeneas library has no model of `u64::saturating_mul`, so `Generated.lean` declares it as
an opaque function. `Epoch::start_slot` calls it. This file states its standard specification,
and nothing more: the result is the product, capped at `u64::MAX`. Rust documents
`saturating_mul` the same way, and it never panics.

This is the only axiom with a specification that the proofs add. `Axioms.lean` pins the
list.
-/

namespace SlotScheduleProofs

open Aeneas Aeneas.Std Aeneas.Std.WP

@[step]
axiom u64_saturating_mul_spec (x y : U64) :
    types.core.num.U64.saturating_mul x y ⦃ r => r.val = min (x.val * y.val) U64.max ⦄

end SlotScheduleProofs
