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

## Trusted base for all caches

- Charon and Aeneas.
- The Aeneas Lean library. Its four `sorry` warnings are not reachable from these theorems.
- The reference matches pyspec.
