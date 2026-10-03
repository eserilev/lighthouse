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

## EpochCache

Files:

- Rust: `../src/state/{total_active_balance,base_rewards}.rs`
- Reference: `CacheProofs/Spec/EpochCache.lean`
- Theorems: `CacheProofs/Equiv/EpochCache.lean`

Production code that calls the pure function:

- `EpochCache::get_effective_balance` and `EpochCache::get_base_reward` (`../src/state/epoch_cache.rs`)
- `PreEpochCache::update_effective_balance` and `PreEpochCache::into_epoch_cache`
  (`consensus/state_processing/src/epoch_cache.rs`)
- `SqrtTotalActiveBalance::new`, `base::get_base_reward`, `altair::get_base_reward` and
  `get_base_reward_per_increment` (`consensus/state_processing/src/common/{base,altair}.rs`)
- `BeaconState::compute_total_active_balance_slow` and `BeaconState::set_total_active_balance`
  (the floor only)

The pure function `integer_sqrt` is the Newton loop of the spec `integer_squareroot`. It replaces the
`integer_sqrt` crate at these call sites. Both return the floor of the square root, so the values
do not change.

### Theorems

`act i` is the "active in the next epoch" flag of validator `i`. `activeSum act ebs` is the sum of
`ebs[i]` over the indices with `act i`.

Total active balance:

| Theorem | Kind | Statement |
|---|---|---|
| `update_effective_balance_spec` | Preservation | If `total = activeSum act ebs`, `i ≤ len`, the flag is `act i` and `total + eb` fits, the update returns `Ok(true)` and keeps `total' = activeSum act ebs'`. A push appends, a late update replaces. |
| `update_effective_balance_out_of_bounds` | Error | If `i > len`, the update returns `Ok(false)` (`ValidatorIndexOutOfBounds`) and changes nothing. |
| `lhRun_spec`, `lhRun_build` | Build | The `single_pass.rs` sequence of calls from an empty cache does not fail, and gives `total = activeSum act ebs`. |
| `single_pass_total_active_balance` | Read | If the calls give the effective balances of `vs`, then `floor_total_active_balance(total)` equals the spec `get_total_active_balance` of the state with validators `vs` at the next epoch. |
| `total_active_balance_equiv` | Read | The same, for any coherent `(ebs, total)`. |
| `compute_total_active_balance_slow_equiv` | Build | The `BeaconState` total active balance cache equals the spec `get_total_active_balance`, and its loop does not overflow if the spec sum does not. |

Base rewards:

| Theorem | Kind | Statement |
|---|---|---|
| `integer_sqrt_equiv` | Read | For every `u64`, the pure function `integer_sqrt` returns `ok`, and its result equals the spec `integer_squareroot`. Both equal `Nat.sqrt`. |
| `base_rewards_spec` | Build | `base_rewards` (the table of `into_epoch_cache`) returns `Ok` with `max_effective_balance / increment + 1` entries. Entry `k` is the base reward for `k * increment` with the floored total. |
| `get_base_reward_equiv` | Read | Build the table from a total whose floor is the spec total active balance. Then for each validator with an effective balance that is a multiple of `EFFECTIVE_BALANCE_INCREMENT` and at most `max_effective_balance`, `get_base_reward` returns `Ok`. The value equals the spec phase0 or altair `get_base_reward`. |
| `get_effective_balance_spec`, `get_effective_balance_no_false_error` | Read | `get_effective_balance` returns `Ok(ebs[i])` for each `i < len`. It returns `ValidatorIndexOutOfBounds` only for `i ≥ len`. |
| `mainnet_hypotheses` | Check | The size hypotheses hold on mainnet for 32 ETH and for 2048 ETH (Electra). |
| `floor_required` | Check | With no active validator, the raw total is 0. Without the floor, `base_reward_per_increment` fails with `DivisionByZero` (`no_floor_division_by_zero`). The spec returns `ok` on the same state. With the floor, `base_rewards` returns `Ok`. This is the fault that #9106 fixed. |

The main statement:

```lean
theorem get_base_reward_equiv (cfg : Config) (vs : List Validator) (epoch : Nat)
    (total inc maxEb factor bpe : U64) (phase0 : Bool)
    (hinc : inc.val = cfg.EFFECTIVE_BALANCE_INCREMENT)
    (hfactor : factor.val = cfg.BASE_REWARD_FACTOR)
    (hbpeq : bpe.val = cfg.BASE_REWARDS_PER_EPOCH)
    (hinc1 : 1 ≤ inc.val) (hbpe1 : 1 ≤ bpe.val)
    (hincf : inc.val * factor.val ≤ U64.max) (hfit : maxEb.val * factor.val ≤ U64.max)
    (hcap : maxEb.val / inc.val < 4294967295)
    (htotal : get_total_active_balance cfg ⟨vs, epoch⟩ = .ok (max inc.val total.val))
    (ebs : Slice U64) (hebs : ebs.val.map (·.val) = vs.map (·.effective_balance))
    (index : Usize) (v : Validator) (hv : vs[index.val]? = some v)
    (hmult : v.effective_balance % inc.val = 0) (hle : v.effective_balance ≤ maxEb.val) :
    ∃ tbl, base_rewards total inc maxEb factor bpe phase0 = ok (.Ok tbl) ∧
      ∃ r, get_base_reward ebs (alloc.vec.Vec.deref tbl) inc index = ok (.Ok r) ∧
        (if phase0 then get_base_reward_phase0 cfg ⟨vs, epoch⟩ index.val
          else get_base_reward_altair cfg ⟨vs, epoch⟩ index.val) = .ok r.val
```

### The reference

`CacheProofs/Spec/EpochCache.lean` transcribes `integer_squareroot`, `is_active_validator`,
`get_active_validator_indices`, `get_total_balance`, `get_total_active_balance`, the phase0
`get_base_reward`, and the altair `get_base_reward_per_increment` and `get_base_reward`. The
local spec checkout is v1.7.0-beta.0-17-g593604b8f. These functions are the same in
v1.7.0-beta.2. The state has only the fields that these functions read.

### Trusted base

- Charon, Aeneas and the Aeneas Lean library. Its `sorry` warnings are not reachable from
  these theorems.
- `lhRun` models the glue in `single_pass.rs`: one `update_effective_balance` call per event,
  in order, and `?` stops at the first error.
- `lhSlowSum` models the loop of `compute_total_active_balance_slow`.
- `single_pass.rs` makes calls that satisfy `ValidCalls`:
  - The main loop calls `update_effective_balance` once for each index `0..n`, in order. The
    dummy update for a validator in a consolidation counts as this call.
  - New validators from pending deposits get the next indices, in order.
  - A late update (consolidations) uses an index below the length.
  - All calls for one index pass the same flag. The flag is `is_active_at(next_epoch)` of the
    final validator. This is true because registry updates for a validator run before its
    first call, and nothing later in the epoch transition changes its activation or exit epoch.
  - Each running total plus the new effective balance fits in a `u64`. The total ETH supply
    (about 2^57 Gwei) is far below 2^64.
- `Validator::is_active_at` equals `is_active_validator`.
- The `ChainSpec` values `effective_balance_increment`, `base_reward_factor` and
  `base_rewards_per_epoch` equal the spec constants. `max_effective_balance_for_fork` of the
  cache epoch is at least the effective balance of each validator. `ForkName::Base` selects the
  phase0 formula.
- Each effective balance is a multiple of `EFFECTIVE_BALANCE_INCREMENT` (spec invariant). Only
  the phase0 formula needs this.
- For the read theorems, the cached effective balances equal the effective balances of the
  state. Effective balances change only in `process_effective_balance_updates`, which writes
  the next cache. `upgrade_to_electra` changes the effective balance of pre-activation
  validators, but it also resets the epoch cache.
- The cache passes `total` to `base_rewards`. Its floor is the spec total active balance. The
  `single_pass.rs` total is covered by `single_pass_total_active_balance`. The
  `initialize_epoch_cache` total comes from the `BeaconState` cache, which is covered by
  `compute_total_active_balance_slow_equiv`.
- `EpochCache::check_validity`, the `CacheNotInitialized` check and the error mapping from the
  pure function errors to `EpochCacheError` are glue.

### Which indices the STF reads

`get_effective_balance` and `get_base_reward` fail for an index at or past the cache length.
The cache has one entry for each validator that existed when it was built. Before Electra, a
deposit in a block adds a validator in the middle of an epoch. That validator has no cache
entry. No caller reads it:

- `process_attestation` (`process_operations.rs`) reads the attesting indices. They are
  committee members, so they are active. An active validator existed before the cache was
  built.
- `process_epoch_single_pass` reads `get_base_reward(index)` only if the validator is eligible
  (active in the previous epoch, or slashed and not yet withdrawable). A new validator is
  neither, because a validator must be active to be slashed.
- After Electra, new validators come only from pending deposits in epoch processing. They are
  pushed to the next cache.

## ProgressiveBalancesCache

The cache keeps, for the previous and the current epoch and for each participation flag, the
total effective balance of the unslashed validators that are active in that epoch and have the
flag. `Balance::get` returns `max(EFFECTIVE_BALANCE_INCREMENT, total)`.

Files:

- Rust: `consensus/types/src/state/{balance,participation_totals}.rs`. It holds
  `Balance` (moved from `state/balance.rs`) and the flag updates. `EpochTotalBalances` and
  `update_flag_total_balances` call it.
- Reference: `CacheProofs/Spec/ProgressiveBalances.lean`.
- Proofs: `CacheProofs/Equiv/ProgressiveBalances.lean`.

### The theorems

`flagSum vs ps e f` is the raw total: the sum of `effective_balance` over the validators that
are active in `e`, not slashed, and have flag `f` in `ps`. `Coherent incr s c` says that for
each flag below 3, the current cache holds `(flagSum .. current_epoch_participation ..
current_epoch f, incr)` and the previous cache holds `(flagSum .. previous_epoch_participation
.. get_previous_epoch(s) f, incr)`. `CurrentCoherent` is the first half.

Each theorem says that Lighthouse returns `Ok`, so it does not return an error, and that the
result is coherent.

| Theorem | Statement |
|---|---|
| `build_coherent` | `initialize_progressive_balances_cache` gives a coherent cache if each total fits in a `u64`. |
| `attestation_coherent` | After `set_participation_flag` for a new flag, `on_new_attestation` gives a coherent cache. |
| `slashing_coherent` | After `set_slashed`, `on_slashing` gives a coherent cache. It never fails. |
| `effective_balance_change_coherent` | After `set_effective_balance`, `on_effective_balance_change` keeps the current cache coherent and does not change the previous cache. |
| `epoch_transition_coherent` | From `CurrentCoherent` alone, `on_epoch_transition` gives a cache that is coherent with `next_epoch_participation(s)`. |
| `registry_change_coherent` | A change to validators that keeps each effective balance, slashed flag and activity in both epochs keeps the cache coherent with no hook. |
| `add_validator_coherent` | A new validator with empty participation keeps the cache coherent with no hook. |
| `read_current_eq_spec` | `current_epoch_flag_attesting_balance(f)` returns `get_total_balance(state, get_unslashed_participating_indices(state, f, get_current_epoch(state)))`. |
| `read_previous_eq_spec` | The same for the previous epoch, after the genesis epoch. |
| `spec_total_eq` | The spec computation equals `max(EFFECTIVE_BALANCE_INCREMENT, flagSum)`. |

```lean
theorem attestation_coherent (incr : Nat) (s s' : Spec.BeaconState) (c : LhCache)
    (e j : Nat) (f : Usize) (is_slashed : Bool) (eb : U64) (v : Spec.Validator) (p : Nat)
    (hc : Coherent incr s c) (hspec : Spec.set_participation_flag s e j f.val = .ok s')
    (he : e = Spec.get_previous_epoch s ∨ e = Spec.get_current_epoch s)
    (hepoch : s.current_epoch ≤ U64.max)
    (hv : s.validators[j]? = some v) (hp : (partAt s e)[j]? = some p)
    (hnew : Spec.has_flag p f.val = false) (hactive : Spec.is_active_validator v e = true)
    (hf : f.val < 3) (hslashed : is_slashed = v.slashed) (heb : eb.val = v.effective_balance)
    (hfit : flagSum s'.validators (partAt s' e) e f.val ≤ U64.max) :
    ∃ c', lhOnNewAttestation c e is_slashed f eb = ok (.Ok c') ∧ Coherent incr s' c'

theorem epoch_transition_coherent (incr : U64) (s : Spec.BeaconState) (c : LhCache)
    (hc : CurrentCoherent incr.val s c) (hepoch : s.current_epoch + 1 ≤ U64.max) :
    ∃ c', lhOnEpochTransition c incr = ok (.Ok c') ∧
      Coherent incr.val (Spec.next_epoch_participation s) c'

theorem read_current_eq_spec (incr : Nat) (s : Spec.BeaconState) (c : LhCache) (f : Usize)
    (hc : CurrentCoherent incr s c) (hf : f.val < 3)
    (hlen : s.current_epoch_participation.length = s.validators.length)
    (hfit : max incr (flagSum s.validators s.current_epoch_participation s.current_epoch f.val)
      < Spec.UINT64_SIZE) :
    ∃ x : U64, lhCurrentEpochFlagAttestingBalance c f = ok (.Ok x) ∧
      (Spec.get_unslashed_participating_indices s f.val (Spec.get_current_epoch s) >>=
        Spec.get_total_balance incr s) = .ok x.val
```

The other statements are in the Lean file.

### Why the previous cache needs no effective balance update

`process_effective_balance_updates` changes effective balances. So the previous epoch total of
the spec changes too, and the previous cache is stale until the end of epoch processing.
`process_participation_flag_updates` then drops the previous epoch. `on_epoch_transition`
replaces the previous cache with the current cache. `epoch_transition_coherent` needs only
`CurrentCoherent`, so the stale previous cache has no effect. No code reads the previous cache
between the effective balance updates and the transition: `RewardsAndPenaltiesContext` and
justification read it before, and `process_epoch` clones the cache for the summary before.

### The reference

`Spec/ProgressiveBalances.lean` transcribes `is_active_validator`, `get_previous_epoch`,
`get_active_validator_indices`, `get_total_balance`, `has_flag`, `add_flag`,
`get_unslashed_participating_indices` and `process_participation_flag_updates`. It also has
the lines of `process_attestation`, `slash_validator` and `process_effective_balance_updates`
that change the cache inputs. The local spec checkout is `v1.7.0-beta.0-17-g593604b8f`. These
functions are the same in v1.7.0-beta.2.

### Trusted base

- Charon, Aeneas and the Aeneas Lean library.
- The hand models of the glue: `lhInitialize` and `lhInitLoop`
  (`initialize_progressive_balances_cache`), `lhNewTotals` (`EpochTotalBalances::new`),
  `lhOnNewAttestation`, `lhOnSlashing`, `lhOnEffectiveBalanceChange`, `lhOnEpochTransition`,
  `lhCurrentEpochFlagAttestingBalance`, `lhPreviousEpochFlagAttestingBalance`,
  `lhIsActiveAt` (`Validator::is_active_at`) and `lhPreviousEpoch`
  (`BeaconState::previous_epoch`). `LhCache` is `Inner`. The models use `Nat` epochs with an
  explicit `u64` overflow check.
- The `From<progressive_balances::Error> for BeaconStateError` impl maps each variant to the
  old variant.
- The cache is initialized when a hook runs, and its `current_epoch` equals the state's current
  epoch. `per_block_processing` and `process_epoch_single_pass` call
  `initialize_progressive_balances_cache` first.
- These facts hold in the state when a hook runs. The theorems take them as hypotheses:
  - Each participation list has the same length as `validators`.
  - A participation flag implies that the validator is active in that epoch. Only committee
    members (active validators) get flags, `process_participation_flag_updates` moves current
    flags to the previous epoch, and no exit or activation epoch changes to a value at or
    before the current epoch.
  - Attestation: the target epoch is the previous or the current epoch. The flag was not set
    before (Lighthouse calls the hook only then). The validator is active in the target epoch.
    `epoch_cache().get_effective_balance(index)` equals `validator.effective_balance`, and
    `slashings_cache().is_slashed(index)` equals `validator.slashed`.
  - Slashing: the validator was not slashed (`is_slashable_validator`). The hook reads the
    effective balance of the validator.
  - Effective balance change: the hook reads `validator.slashed` and the old effective balance.
  - Each total fits in a `u64` (about 18.4 billion ETH).
- No other code changes `effective_balance`, `slashed` or a participation flag of an active
  validator. A search of `consensus/` finds only these writes: `single_pass.rs` (with the
  hook), `slash_validator.rs` (with the hook), `process_operations.rs` (with the hook),
  `effective_balance_updates.rs` (phase0 only), `genesis.rs` and `upgrade/altair.rs` (before
  `initialize`), `upgrade/electra.rs` (validators that are not active), and new validators
  (`add_validator_coherent`).

### Genesis epoch

At the genesis epoch, `get_previous_epoch(state)` is the current epoch. So the spec reads
`current_epoch_participation` for the previous epoch. The previous cache holds the total over
`previous_epoch_participation`, which is empty at genesis. The two differ when there are
attestations in the genesis epoch. The spec does not use the previous totals at genesis
(justification returns early, and rewards skip genesis), so consensus is not affected.
`read_previous_eq_spec` has the hypothesis `GENESIS_EPOCH < current_epoch`.

## Trusted base for all caches

- Charon and Aeneas.
- The Aeneas Lean library. Its four `sorry` warnings are not reachable from these theorems.
- The reference matches pyspec.
