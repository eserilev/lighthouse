# Epoch processing proofs

Goal: prove that Lighthouse's Gloas epoch processing equals the consensus spec for every state.

```
../src/per_epoch_processing/{builder_pending_payments,effective_balance}.rs
   --charon--> .llbc --aeneas--> EpochProofs/Generated.lean
                                          |
                     EpochProofs/Equiv/*.lean (equivalence proof)
                                          |
EpochProofs/Spec/*.lean  <-- written by hand from the consensus specs
```

## Status

| Function | Reference | Generated | Equivalence |
|---|---|---|---|
| `process_builder_pending_payments` | Done | Done | Done |
| `process_effective_balance_updates` (per validator) | Done | Done | Done |

## The theorems

```lean
theorem builder_pending_payments_equiv (p : Spec.Preset)
    (total_active_balance slots_per_epoch : U64) (spe : Usize)
    (hslots : slots_per_epoch.val = p.SLOTS_PER_EPOCH) (hspe : spe.val = p.SLOTS_PER_EPOCH)
    (payments : Slice LhPayment) (withdrawals : List LhWithdrawal)
    (hlen : payments.val.length = 2 * p.SLOTS_PER_EPOCH) :
    lighthouseBuilderPendingPayments total_active_balance slots_per_epoch spe payments withdrawals
      ⦃ r => absResult absState r =
        Spec.process_builder_pending_payments p total_active_balance.val
          (absState (withdrawals, payments.val)) ⦄

theorem new_effective_balance_equiv (p : Spec.Preset) (validator : Spec.Validator)
    (balance effective_balance limit downward upward increment : U64)
    (heffective : effective_balance.val = validator.effective_balance)
    (hlimit : limit.val = Spec.get_max_effective_balance p validator)
    (hincrement : increment.val = p.EFFECTIVE_BALANCE_INCREMENT) :
    per_epoch_processing.effective_balance.new_effective_balance
      balance effective_balance limit downward upward increment ⦃ r =>
        absResult (·.val) r =
          Spec.newEffectiveBalance p downward.val upward.val validator balance.val ⦄
```

For every input, Lighthouse returns `ok`, so it does not panic. Its result maps to the
reference result. An overflow or a division by zero maps to the same spec error.

`hysteresis_thresholds_equiv` does the same for the threshold computation.

`absState` maps Aeneas types to reference types. Rust `Default` maps to spec `empty()`.

## The reference

`EpochProofs/Spec/` transcribes the consensus specs (v1.7.0-beta.2) line by line. Each
definition quotes its pyspec. Effective balance updates come from Electra, because Gloas does
not change them.

Rules:

- Write the reference from the spec only. Do not read Lighthouse.
- Keep the spec names and statement order. Do not optimise.
- Model each pyspec raise as `Except.error`. Use the `uint64*` functions for `Uint64` values.
- Python `or` does not evaluate its right side when the left side is true. Keep that order.
- Import Lean core only.

`EpochProofs/Sanity/` proves facts about the reference alone:

| Theorem | Fact |
|---|---|
| `withdrawals_loop` | The loop appends the withdrawals of payments at or above the quorum, in order. It never fails. |
| `process_builder_pending_payments_eq` | A closed form with no loop and no slice assignment |
| `process_builder_pending_payments_wellFormed` | The payments vector keeps length `2 * SLOTS_PER_EPOCH` |
| `get_builder_payment_quorum_threshold_ok` | If `SLOTS_PER_EPOCH ≥ 6`, the quorum does not overflow |
| `process_effective_balance_updates_eq` | The loop is a `mapM` of one step per validator |
| `hysteresisThresholds_mainnet` | Mainnet thresholds are 0.25 ETH down and 1.25 ETH up |
| `newEffectiveBalance_le_max` | A changed effective balance never exceeds `get_max_effective_balance` |
| `newEffectiveBalance_in_band` | Inside the hysteresis band, the effective balance does not change |

`get_total_active_balance(state)` is a parameter. Lighthouse reads it from a cache.

## Building

```sh
cd consensus/state_processing/proofs
lake exe cache get
lake build
lake env lean EpochProofs/Axioms.lean
```

## Regenerating after a change to a translated module

Use the Charon and Aeneas revisions in `.github/workflows/proofs.yml`. See
`validator_client/slashing_protection/proofs/README.md` for the build steps.

```sh
cd consensus/state_processing
out=$(mktemp -d)
charon cargo --preset=aeneas \
  --start-from 'state_processing::per_epoch_processing::builder_pending_payments' \
  --start-from 'state_processing::per_epoch_processing::effective_balance' \
  --include safe_arith --include types::builder --include alloy_primitives::bits \
  --dest-file "$out/pure.llbc" -- --lib
aeneas -backend lean "$out/pure.llbc" -dest "$out"
cp "$out/Pure.lean" proofs/EpochProofs/Generated.lean
```

The `--include` flags translate the bodies of the `safe_arith` functions and the derived `Clone`
and `Default` impls. Without them, these functions become axioms.

If Charon fails with `found crate core compiled by an incompatible version of rustc`, the cached
Miri sysroot is for another nightly. Set `CHARON_MIRI_SYSROOTS` to a sysroot for the nightly in
Charon's `rust-toolchain` file.

## Why the translated modules look the way they do

Aeneas rejected the original code in `single_pass.rs` for two reasons:

- `push(...)?` inside a `for` loop is an early return inside a loop. Aeneas drops the body.
- Iterator adapters (`take`, `filter`, `map`, `chain`) stop Aeneas with an internal error.

`state_processing` denies indexing and unchecked arithmetic, so the module uses `get` and
`saturating_add`.

`effective_balance.rs` takes plain `u64` values. If a function takes `&Validator` or
`&ChainSpec`, Aeneas turns `Epoch`, `Slot`, `PublicKey`, `Option::map` and more into axioms.

## Trusted base

- Charon and Aeneas.
- The Aeneas Lean library. Its four `sorry` warnings are not reachable from these theorems.
- The glue in `single_pass.rs`. It copies the milhouse vector, calls the module, and writes the
  results back. `lighthouseBuilderPendingPayments` models it by hand.
- `ChainSpec` sets the payment threshold to 6/10. The glue model uses these values.
- `state_ctxt.total_active_balance` equals `get_total_active_balance(state)`.
- `Validator::get_max_effective_balance` equals the spec `get_max_effective_balance` after
  Electra. `single_pass.rs` passes its result to `new_effective_balance`.
- Lighthouse computes the hysteresis thresholds once per epoch. The spec computes them once per
  validator. The values are the same.
- The reference matches pyspec.
