import EpochProofs.Spec.Types

/-!
# Reference: `process_builder_pending_payments`

Transcribed line by line from `specs/gloas/beacon-chain.md` (v1.7.0-beta.2).
Each definition quotes its pyspec.

`get_total_active_balance(state)` is a parameter. The reference does not model the
validator registry yet.
-/

namespace EpochProofs.Spec

/-- ```python
def get_builder_payment_quorum_threshold(state: BeaconState) -> Uint64:
    per_slot_balance = get_total_active_balance(state) // Uint64(SLOTS_PER_EPOCH)
    quorum = per_slot_balance * BUILDER_PAYMENT_THRESHOLD_NUMERATOR
    return Uint64(quorum // BUILDER_PAYMENT_THRESHOLD_DENOMINATOR)
``` -/
def get_builder_payment_quorum_threshold (p : Preset) (total_active_balance : Gwei) :
    SpecM Uint64 := do
  let per_slot_balance ← uint64Div total_active_balance p.SLOTS_PER_EPOCH
  let quorum ← uint64Mul per_slot_balance BUILDER_PAYMENT_THRESHOLD_NUMERATOR
  uint64Div quorum BUILDER_PAYMENT_THRESHOLD_DENOMINATOR

/-- ```python
def process_builder_pending_payments(state: BeaconState) -> None:
    quorum = get_builder_payment_quorum_threshold(state)
    for payment in state.builder_pending_payments[:SLOTS_PER_EPOCH]:
        if payment.weight >= quorum:
            state.builder_pending_withdrawals.append(payment.withdrawal)

    old_payments = state.builder_pending_payments[SLOTS_PER_EPOCH:]
    state.builder_pending_payments[:SLOTS_PER_EPOCH] = old_payments
    new_payments = [BuilderPendingPayment.empty() for _ in range(SLOTS_PER_EPOCH)]
    state.builder_pending_payments[SLOTS_PER_EPOCH:] = new_payments
```

The slice assignments use Python list rules. `a[:k] = b` is `b ++ a.drop k`.
`a[k:] = c` is `a.take k ++ c`. -/
def process_builder_pending_payments (p : Preset) (total_active_balance : Gwei)
    (state : BeaconState) : SpecM BeaconState := do
  let quorum ← get_builder_payment_quorum_threshold p total_active_balance
  let mut builder_pending_withdrawals := state.builder_pending_withdrawals
  for payment in state.builder_pending_payments.take p.SLOTS_PER_EPOCH do
    if payment.weight ≥ quorum then
      builder_pending_withdrawals := builder_pending_withdrawals ++ [payment.withdrawal]

  let mut builder_pending_payments := state.builder_pending_payments
  let old_payments := builder_pending_payments.drop p.SLOTS_PER_EPOCH
  builder_pending_payments := old_payments ++ builder_pending_payments.drop p.SLOTS_PER_EPOCH
  let new_payments := List.replicate p.SLOTS_PER_EPOCH BuilderPendingPayment.empty
  builder_pending_payments := builder_pending_payments.take p.SLOTS_PER_EPOCH ++ new_payments

  pure { state with builder_pending_withdrawals, builder_pending_payments }

end EpochProofs.Spec
