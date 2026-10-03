# BeaconState cache proofs

Goal: prove that each `BeaconState` cache agrees with the consensus spec.

```
pure functions in ../src (the modules listed in regenerate.sh)
   --charon--> .llbc --aeneas--> CacheProofs/Generated.lean
                                          |
                     CacheProofs/Equiv/*.lean (the theorems)
                                          |
CacheProofs/Spec/*.lean  <-- written by hand from the consensus specs
```

## Method

Each cache calls pure functions in a module next to it, for example `../src/state/exit_queue.rs`
for `ExitCache`. Aeneas translates these modules. Each cache gets three kinds of theorem:

- Build: the cache built from scratch equals a summary of the state written from the spec.
- Preservation: an incremental update on a correct cache equals a rebuild from the changed
  state.
- Read agreement: a cache query equals the spec computation. If the spec succeeds, Lighthouse
  does not return an error.

Glue code that Aeneas does not translate is modelled by hand in Lean. These models are named
`lh*` and are part of the trusted base.

## Building

```sh
cd consensus/types/proofs
lake exe cache get
lake build
lake env lean CacheProofs/AxiomAudit.lean
```

`CacheProofs/AxiomAudit.lean` checks every theorem under `CacheProofs`. It fails if a theorem
depends on an axiom other than `propext`, `Classical.choice` and `Quot.sound`, or on `sorry`.

## Regenerating after a change to a pure function

Build Charon and Aeneas at the revisions in `.github/workflows/lean-proofs.yml` and
`lakefile.toml`. Then run:

```sh
CHARON=/path/to/charon AENEAS=/path/to/aeneas ./regenerate.sh
```

If Charon fails with `found crate core compiled by an incompatible version of rustc`, set
`CHARON_MIRI_SYSROOTS` to a Miri sysroot for the nightly in Charon's `rust-toolchain` file.

## The reference

`CacheProofs/Spec/` transcribes the consensus specs (v1.7.0-beta.2). Each definition quotes
its pyspec. Write it from the spec only. Do not read Lighthouse. Each pyspec raise is
`Except.error`.

## ExitCache

Rust: `../src/state/exit_queue.rs` (`record_exit`, `churn_at`).
Reference: the exit queue epoch in phase0 `initiate_validator_exit` (`Spec/ExitCache.lean`).

```lean
theorem build_eq (es : List U64) (hlen : es.length ≤ 18446744073709551615) :
    ∃ s, lhBuild es = ok (.Ok s) ∧
      (s.1.val, s.2.val) = closedForm ((es.map (·.val)).filter nonFar)

theorem exit_queue_epoch_equiv (es : List U64) (delayed limit : U64)
    (hdelayed : delayed.val < Spec.FAR_FUTURE_EPOCH)
    (hlen : es.length ≤ 18446744073709551615) :
    ∃ s, lhBuild es = ok (.Ok s) ∧
      (do let r ← lhExitQueueEpoch s.1 s.2 delayed limit; ok (absExit r)) =
        ok (Spec.exitQueueEpoch (es.map (·.val)) delayed.val limit.val)

theorem build_set_record (es : List U64) (i : Nat) (e : U64) (hi : i < es.length)
    (hfar : es[i].val = Spec.FAR_FUTURE_EPOCH) (he : e.val ≠ Spec.FAR_FUTURE_EPOCH)
    (hlen : es.length ≤ 18446744073709551615) :
    ∃ s r s', lhBuild es = ok (.Ok s) ∧
      state.exit_queue.record_exit s.1 s.2 e = ok (.Ok r) ∧
      lhBuild (es.set i e) = ok (.Ok s') ∧ r.1.val = s'.1.val ∧ r.2.val = s'.2.val
```

`es` is the `exit_epoch` of each validator, in order.

- `build_eq`: `ExitCache::new` never fails. It holds the largest exit epoch below
  `FAR_FUTURE_EPOCH` (or 0), and the number of validators that exit at that epoch.
- `exit_queue_epoch_equiv`: the exit queue epoch from the cache equals the spec value. This
  includes the overflow error of `exit_queue_epoch += 1`. Lighthouse never returns
  `ExitCacheInvalidEpoch`.
- `build_set_record`: when a validator exits, `record_exit` on the old cache equals a rebuild
  from the new exit epochs. So the cache stays correct after each exit.

Trusted base:

- `lhBuild` models `ExitCache::new`. `lhExitQueueEpoch` models the pre-Electra branch of
  `initiate_validator_exit` and the same code in `single_pass.rs`.
- `delayed` equals `compute_activation_exit_epoch(get_current_epoch(state))` and is below
  `FAR_FUTURE_EPOCH`. `limit` equals `get_validator_churn_limit(state)`.
- `ChainSpec::far_future_epoch` is `2^64 - 1`.
- Before Electra, each code path that sets `exit_epoch` calls `record_validator_exit`, once
  per exit, with an epoch below `FAR_FUTURE_EPOCH`.
- After Electra, Lighthouse does not read this cache. The exit queue epoch comes from
  `compute_exit_epoch_and_update_churn`. A consolidation sets `exit_epoch` without a call to
  `record_validator_exit` (`process_operations.rs`, `process_consolidation_request`), so the
  cache is not correct after Electra.

## ActivationQueue

`ActivationQueue` (`src/state/activation_queue.rs`) is a speculative activation queue in the
`EpochCache`. It is a `BTreeSet<(Epoch, usize)>` of `(activation_eligibility_epoch, index)`.

### When Lighthouse builds and reads it

The cache for epoch `E` comes from one of two builders:

- A. `initialize_epoch_cache` (`state_processing/src/epoch_cache.rs:170`) runs during epoch `E`.
  It adds each validator with `could_be_eligible_for_activation_at(E + 1)`.
- B. Single-pass epoch processing at the end of epoch `E - 1`
  (`single_pass.rs:860`). It adds each validator after its own registry update, with
  `could_be_eligible_for_activation_at(E)`.

`process_registry_updates` at the end of epoch `E` reads it with
`get_validators_eligible_for_activation(finalized_epoch, churn_limit)`
(`single_pass.rs:231`, `registry_updates.rs:50`). Electra and later do not read it.

Between the build and the read:

- Deposits append validators with `activation_eligibility_epoch = FAR_FUTURE_EPOCH`.
- The eligibility loop of `process_registry_updates` at the end of `E` sets
  `activation_eligibility_epoch` from `FAR_FUTURE_EPOCH` to `E + 1`.
- No activation epoch changes. Exits and slashings write only exit fields.
- Finality changes. The queue does not depend on it.

`Frame finalized_epoch vb vr` states these facts. `vb` is the validator list that the builder
sees. `vr` is the list at the read.

### The theorems

`CacheProofs/Equiv/ActivationQueue.lean`. `nomega` is `omega` after it unfolds the `Epoch`
abbrev.

```lean
-- Pure functions (Aeneas translation of src/validator/activation_eligibility.rs)
theorem could_be_eligible_for_activation_at_equiv ... :
    could_be_eligible_for_activation_at elig act epoch far = ok (couldBeAt epoch.val validator)
theorem is_eligible_for_activation_equiv ... :
    is_eligible_for_activation elig act fin far = ok (Spec.is_eligible_for_activation fin.val validator)

-- Build: both builders give the sorted keys of the validators with
-- could_be_eligible_for_activation_at(epoch).
theorem lhBuild_eq_queueSummary (epoch : Nat) (validators : List Validator) :
    lhBuild epoch validators = queueSummary epoch validators

-- Why the cache works.
theorem couldBeAt_of_is_eligible_for_activation (hfin : finalized_epoch < epoch)
    (h : is_eligible_for_activation finalized_epoch validator = true) :
    couldBeAt epoch validator = true
theorem over_approximation (hframe : Frame finalized_epoch vb vr) (hfin : finalized_epoch < epoch)
    (hr : vr[i]? = some r) (hel : is_eligible_for_activation finalized_epoch r = true) :
    ∃ b, vb[i]? = some b ∧ couldBeAt epoch b = true ∧
      b.activation_eligibility_epoch = r.activation_eligibility_epoch

-- Read agreement, phase0 to Deneb.
theorem lhSelect_eq_dequeued (hframe : Frame finalized_epoch vb vr)
    (hfin : finalized_epoch < epoch) (hepoch : epoch ≤ FAR_FUTURE_EPOCH) :
    lhSelect (lhBuild epoch vb) finalized_epoch churn_limit =
      dequeued finalized_epoch churn_limit vr
theorem mem_lhSelect_iff ... : i ∈ lhSelect (lhBuild epoch vb) .. ↔ i ∈ dequeued ..

-- Read agreement, Electra: three `if`s in Lighthouse equal `if`/`elif` in the spec.
theorem lhRegistryUpdatePostElectra_eq (hfin : finalized_epoch ≤ current_epoch)
    (hcur : current_epoch < FAR_FUTURE_EPOCH)
    (hbal : EJECTION_BALANCE < MIN_ACTIVATION_BALANCE) :
    lhRegistryUpdatePostElectra .. validator = process_registry_update_electra .. validator

-- Discharge of the hypotheses from the spec.
theorem process_activation_eligibility_frame (hfin : finalized_epoch ≤ current_epoch)
    (h : process_activation_eligibility MAX_EFFECTIVE_BALANCE current_epoch vs = .ok vs') :
    vs'.length = vs.length ∧ ∀ i b r, .. -- the Frame facts for the eligibility loop
theorem process_finalizations_lt (hfin : finalized_epoch < current_epoch)
    (h : process_finalizations bits pj cj current_epoch finalized_epoch = .ok f) :
    f < current_epoch
```

`hfin` holds on both paths:

- A: `epoch = E + 1`, and the finalized epoch is at most `E`.
- B: `epoch = E`. At the end of epoch 0 the finalized epoch is 0. At the end of a later epoch
  `E`, the old finalized epoch is less than `E`. `process_finalizations_lt` keeps it less than
  `E`. By induction, the finalized epoch at the read is less than `E` for `E ≥ 1`.

The functions have no error path. `get_validators_eligible_for_activation` returns indices from
`vb`, and `vb.length ≤ vr.length`. So `get_validator_mut(index)?` does not fail.

### The reference

`CacheProofs/Spec/ActivationQueue.lean` transcribes `is_active_validator`,
`is_eligible_for_activation_queue` (phase0, Electra), `is_eligible_for_activation`,
`process_registry_updates` (phase0 to Deneb, Electra), and the finalization rules of
`weigh_justification_and_finalization`. The local spec checkout is v1.7.0-beta.0-17-g593604b8f.
These functions are the same in v1.7.0-beta.2.

The reference omits `initiate_validator_exit`. It writes only `exit_epoch` and
`withdrawable_epoch`, and the activation functions do not read them.

### Trusted base

- Charon and Aeneas. No Charon flags beyond `--include safe_arith`.
- `BTreeSet<(Epoch, usize)>` is a sorted list without duplicates. `lhInsert` models `insert`.
  Iteration is in list order. The tuple order is lexicographic, as `tupleLe`.
- `lhBuild` models the loops in `initialize_epoch_cache` and
  `process_single_registry_update_pre_electra`. `lhSelect` models
  `get_validators_eligible_for_activation` before `collect`. The `collect` into
  `BTreeSet<usize>` keeps the members. Single-pass uses `contains`, and `registry_updates.rs`
  sets the same activation epoch for each member, so order does not matter.
- `lhRegistryUpdatePostElectra` models `process_single_registry_update_post_electra`.
  `Validator::is_eligible_for_activation_queue` and `Validator::is_active_at` equal the spec
  functions.
- `Frame` holds between the build and the read. Block processing before Electra appends
  validators with `FAR_FUTURE_EPOCH` eligibility and does not write `activation_epoch` or
  `activation_eligibility_epoch` of existing validators.
- Lighthouse's justification and finalization equals the spec. `finalized_epoch` is the epoch
  after it.
- `get_activation_churn_limit` equals `get_validator_activation_churn_limit` (Deneb) or
  `get_validator_churn_limit` (before Deneb).
- Mainnet and minimal presets have `EJECTION_BALANCE < MIN_ACTIVATION_BALANCE`.

### Coherence notes

- `process_epoch_single_pass` with `effective_balance_updates` and without `registry_updates`
  stores an empty queue for the next epoch (`single_pass.rs:480-486`). Only
  `process_effective_balance_updates_slow` (tests) does this. A real epoch transition enables
  all steps.
- Builder A uses `E + 1` and builder B uses `E`. Both over-approximate. A keeps more entries,
  and the filter removes them.

## Trusted base for all caches

- Charon and Aeneas.
- The Aeneas Lean library. Its four `sorry` warnings are not reachable from these theorems.
- The reference matches pyspec.
