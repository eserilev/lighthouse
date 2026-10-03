# Epoch processing proofs

Goal: prove that Lighthouse's Gloas epoch processing equals the consensus spec for every state.

```
../src/per_epoch_processing/{builder_pending_payments,effective_balance,inactivity_updates,slashings_penalty,
   rewards_penalties,registry_update,pending_deposits}.rs
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
| `process_inactivity_updates` (per validator) | Done | Done | Done |
| `process_slashings` (context and per validator) | Done | Done | Done |
| `process_rewards_and_penalties` (per validator) | Done | Done | Done, with a condition |
| `process_registry_updates` (per validator, after Electra) | Done | Done | Done, with conditions |
| `process_pending_deposits` (decisions) | Done | Done | Done |

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

theorem new_inactivity_score_equiv (p : Spec.Preset) (score bias recovery_rate : U64)
    (is_eligible is_participating is_in_leak : Bool)
    (hbias : bias.val = p.INACTIVITY_SCORE_BIAS)
    (hrecovery : recovery_rate.val = p.INACTIVITY_SCORE_RECOVERY_RATE) :
    per_epoch_processing.inactivity_updates.new_inactivity_score
      score is_eligible is_participating is_in_leak bias recovery_rate ⦃ r =>
        absResult (·.val) r =
          if is_eligible then Spec.inactivityScoreStep p is_participating is_in_leak score.val
          else .ok score.val ⦄
```

For every input, Lighthouse returns `ok`, so it does not panic. Its result maps to the
reference result. An overflow or a division by zero maps to the same spec error.

`hysteresis_thresholds_equiv` does the same for the threshold computation.
`slashings_context_equiv` and `new_balance_after_slashing_equiv` do the same for slashings,
after Electra.

`new_balance_after_rewards_equiv` relates `new_balance_after_rewards` to `rewardsCombined`: add all
rewards, then subtract all penalties once. The spec instead applies the four deltas in four
rounds, with `saturating_sub` after each round (`rewardsSequential`).
`new_balance_after_rewards_eq_spec` shows that the two agree when the balance covers all
penalties and no addition overflows. `rewards_saturation_example` shows an input where they
differ: a balance of 3, a source penalty of 5 and a target reward of 10 give 10 in the spec
and 8 in Lighthouse. Prysm uses the same order as Lighthouse.

`registry_update_equiv` relates `registry_update` to `registryStepIndependent`: three
independent steps (queue eligibility, ejection, activation). The spec uses `if`/`elif`/`elif`
instead (`registryStepExclusive`). `registry_update_eq_spec` shows that the two agree when
`EJECTION_BALANCE < MIN_ACTIVATION_BALANCE`, the finalized epoch is not after the current
epoch, and the epochs fit in a `u64`. The theorems cover Gloas, where the exit churn has no
upper limit.

`process_pending_deposits_equiv` relates the deposit decisions to `depositDecisions`: which
deposits to apply, which to postpone, where to stop, and the remaining churn. Lighthouse reads
each deposit's validator before `process_registry_updates` runs and predicts its ejection.
`predictedStatus_eq_post_registry` shows that this prediction equals the spec's flags after the
registry update, on valid states.

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
| `process_inactivity_updates_eq` | If the eligible set, the participating set and the leak flag evaluate, the loop is a `foldlM` of one step per eligible index |
| `inactivityScoreStep_leak_missed` | In a leak, a validator that missed the target gains exactly `INACTIVITY_SCORE_BIAS` |
| `inactivityScoreStep_participating_le` | The score of a validator that hit the target never goes up |
| `process_slashings_eq` | The loop is a `foldlM` of one step per validator, with the preamble computed once |
| `slashingBalanceStep_not_slashed` | A validator that is not slashed keeps its balance |
| `slashingBalanceStep_le` | The penalty never raises a balance |
| `rewardsSequential_eq_combined` | Without saturation, the four-round order is `balance + rewards - penalties` |
| `rewardsCombined_eq_sequential` | Under the same condition, the Lighthouse order equals the spec order |
| `rewards_saturation_example` | Without the condition, the two orders differ |
| `registryStepIndependent_eq_exclusive` | On valid states, three independent steps equal the spec's `if`/`elif`/`elif` |
| `predictedStatus_eq_post_registry` | Lighthouse's ejection prediction equals the flags after the registry update |
| `depositLoop_append` | The deposit loop over two lists is the loop over the first, then the second |

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
  --start-from 'state_processing::per_epoch_processing::inactivity_updates' \
  --start-from 'state_processing::per_epoch_processing::slashings_penalty' \
  --start-from 'state_processing::per_epoch_processing::rewards_penalties' \
  --start-from 'state_processing::per_epoch_processing::registry_update' \
  --start-from 'state_processing::per_epoch_processing::pending_deposits' \
  --include safe_arith --include types::builder --include alloy_primitives::bits \
  --include types::core::consts \
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

`registry_update.rs` splits the update into three step functions. In one function, Aeneas
copies the rest of the body into each branch, and the generated code grows exponentially.

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
- `single_pass.rs` passes three flags to `new_inactivity_score`: `is_eligible`, the timely
  target check, and `is_in_inactivity_leak`. Each equals its spec predicate.
- `single_pass.rs` sums `state.slashings` with `safe_sum`. The theorem takes the sum as an input.
- `single_pass.rs` passes `base_reward` from the epoch cache, the participation flags, the leak
  flag and the participating increments from the progressive balances cache to
  `new_balance_after_rewards`. Each equals its spec value.
- `single_pass.rs` passes the validator epochs, the churn state, the finalized epoch and the
  chain constants to `registry_update`. `ConstantsMatch` states the constants.
- `single_pass.rs` builds the deposit views from the pubkey cache, for at most
  `MAX_PENDING_DEPOSITS_PER_EPOCH` deposits, and applies the decisions. The spec reads the
  pubkeys again in each iteration, so a deposit for a validator added earlier in the same loop
  finds it. Lighthouse takes the same path: such a deposit counts as a new validator, consumes
  churn, and is applied to the added validator after the loop.
- `apply_pending_deposit` is a parameter of the reference. It verifies a BLS signature.
- The registry proof covers Electra and later. The pre-Electra path is unchanged and not proved.
- Lighthouse computes the slashings target epoch once per epoch. The spec computes it once per
  slashed validator. The theorems assume that `epoch + EPOCHS_PER_SLASHINGS_VECTOR / 2` fits in a
  `u64`.
- The slashings proof covers Electra and later. The pre-Electra branch is translated but not
  proved.
- Lighthouse computes the hysteresis thresholds once per epoch. The spec computes them once per
  validator. The values are the same.
- The reference matches pyspec.
