# Experimental features with Fresnel

An experimental feature runs a consensus change without a new `ForkName`. The feature gets its own fork version, fork epoch, and config keys. Its logic sits behind gates. Mainnet and the public testnets cannot schedule a feature, so the gated code never runs there.

Fresnel (`fresnel/` at the repo root) is the generator. It reads `consensus/types/src/features/registry.toml` and writes the feature code. It also lints where each feature appears in the code, and it deletes features. EIP-8198 (quick slots) is the worked example in this guide.

Fresnel does not support features that add fields to consensus containers.

## Files

| Path | Contents |
|---|---|
| `consensus/types/src/features/registry.toml` | The feature list. Edit this file. |
| `consensus/types/src/features/generated.rs` | Generated from the registry. Do not edit. |
| `consensus/types/src/features/generated_fixture.rs` | Generated from the registry and `fresnel/tests/fixtures/test_features.toml`. Do not edit. |
| `consensus/types/src/features/mod.rs` | `Feature`, `Active<F>`, `feature_enabled`, `feature_fork_epoch`, `validate_features`. |
| `consensus/types/src/features/<name>/` | Feature code that `types` owns. Fresnel writes `FOOTPRINT.md` here. |
| `consensus/state_processing/src/features/<name>/` | Feature code that `state_processing` owns. |
| `consensus/feature_dispatch` | The `#[feature_dispatch]` attribute macro. |

The `types` tests and the `fresnel-fixture` Cargo feature compile `generated_fixture.rs` in place of `generated.rs`. This gives the tests two test features to schedule: `heze_test_feature` and `gloas_test_feature`.

## What a feature gets

For a registry entry `[eip1234]`, `make features` generates:

- the type `Eip1234` and the ID `FeatureId::Eip1234`.
- the config keys `EIP1234_FORK_VERSION` and `EIP1234_FORK_EPOCH`, in the consensus-specs config format.
- one field in `FeatureSpec` (`spec.features`) and one key in `FeatureConfig` for each `config` entry.

The feature does not get a `ForkName` or superstruct variants. Its states and blocks keep the variant of the real fork that they run on.

A feature is off until a config sets its fork epoch. A missing epoch and `FAR_FUTURE_EPOCH` both mean off.

## Registry format

```toml
[eip1234]
min_fork = "Heze"
fork_version = { mainnet = "0x12340000", minimal = "0x12340001", gnosis = "0x12340064" }
config = [
    { name = "EIP1234_LIMIT", type = "u64" },
    { name = "EIP1234_CAP", type = "u64", optional = true },
]
```

| Field | Required | Rule |
|---|---|---|
| table name | yes | Lower case ASCII letters, digits, and `_`. Starts with a letter. The type name is the camel case form: `eip_1234` becomes `Eip1234`. |
| `min_fork` | yes | A variant of `ForkName`. The first fork that the feature can run on. |
| `fork_version` | yes | A value for each of `mainnet`, `minimal`, and `gnosis`. Each value is `"0x"` and 8 hex digits. |
| `config` | no | A list of config keys. Each entry has `name`, `type`, and an optional `optional`. |

Rules for a `config` entry:

- `name` is the YAML key. It is upper case ASCII letters, digits, and `_`, and starts with a letter. No prefix is necessary.
- The field name in `FeatureSpec` is the lower case form of `name`: `EIP1234_LIMIT` becomes `spec.features.eip1234_limit`.
- `type` is a Rust type, relative to `generated.rs`, for example `crate::core::SlotDurationSchedule`.
- The type must implement `Clone`, `Debug`, `PartialEq`, `Serialize`, and `Deserialize`. Under the `arbitrary` Cargo feature, it must also implement `arbitrary::Arbitrary`.
- Without `optional`, the spec field has type `type` and starts at `Default::default()`. The type must then implement `Default`.
- With `optional = true`, the spec field has type `Option<type>` and starts at `None`. A config that the spec produces leaves the key out while the value is `None`.
- If a config does not set a key, the spec keeps its current value for that key.

## Add a feature

This example adds a feature called `eip1234` that runs on Heze.

1. Add the entry to `registry.toml` (see [Registry format](#registry-format)).
2. Run `make features`.
3. Put the feature code in `features/eip1234/` directories (see [Feature directories](#feature-directories)).
4. Connect the feature to the code with dispatch attributes or inline gates (see [Gate the logic](#gate-the-logic)).
5. Run `make features` again. This writes `consensus/types/src/features/eip1234/FOOTPRINT.md`.
6. Commit `registry.toml`, the generated files, and `FOOTPRINT.md` with the code.
7. Run `make features-check`.
8. Run `cargo check --workspace --tests`.

To run the feature on a devnet, set its keys in the devnet `config.yaml`:

```yaml
EIP1234_FORK_VERSION: 0x12340000
EIP1234_FORK_EPOCH: 10
EIP1234_LIMIT: 8
```

The fork epoch must be at or after the epoch of `min_fork`. Use a fork version that no other fork on that devnet uses. The devnet must have its own genesis fork version.

## Feature directories

Feature code goes in a `features/<name>/` directory, in the crate that owns the code that it changes:

- `consensus/types/src/features/<name>/` for `ChainSpec`, `BeaconState`, config validation, and helpers.
- `consensus/state_processing/src/features/<name>/` for state transition code.

Each directory is a module. Declare it in the parent `features/mod.rs` as `pub mod <name>;`. Put the tests of the feature in the directory too, for example `features/<name>/tests.rs`.

Inside a feature directory, code can use the feature type and `Active<Name>` in any form. Outside the directories, the footprint lint limits each use (see [Footprint lint](#footprint-lint)).

## Gate the logic

Every change in behavior goes behind a gate. A gate is a call to `ChainSpec::feature_enabled`:

```rust
if let Some(on) = spec.feature_enabled::<Eip1234>(epoch) {
    features::eip1234::new_value(state, spec, on)
} else {
    old_value(state, spec)
}
```

`feature_enabled` returns `Some(Active<Eip1234>)` when all three conditions are true:

- the feature has a fork epoch.
- `epoch` is at or after the fork epoch.
- the real fork at `epoch` is `min_fork` or later.

Only `feature_enabled` creates an `Active` token. Give the token as the last argument of each feature function. Then a feature function cannot run without a gate.

There are two ways to write a gate: the `#[feature_dispatch]` attribute and an inline gate.

### Dispatch attribute

Use `#[feature_dispatch]` when the feature replaces the full body of a function:

```rust
#[feature_dispatch(Eip1234 => features::eip1234::process_foo, spec = spec, epoch = state.current_epoch())]
pub fn process_foo<E: EthSpec>(state: &mut BeaconState<E>, spec: &ChainSpec) -> Result<(), Error> {
    // current body
}
```

The macro adds this code at the start of the function:

```rust
let active = spec.feature_enabled::<Eip1234>(state.current_epoch());
if let Some(active) = active {
    return features::eip1234::process_foo(state, spec, active);
}
// current body
```

Rules for the attribute:

- The form is `Name => path, spec = <expr>, epoch = <expr>`. `spec` and `epoch` are both required, in any order.
- The path can have generics, for example `features::eip1234::get_due::<E>`.
- The copy takes the same arguments in the same order, then the `Active<Name>` token. For a method, the copy takes `self` as its first argument.
- The copy returns the same type. A different argument list or return type is a compile error.
- Each argument of the function must be a plain name. A pattern argument, for example `(a, b): (u64, u64)`, is a compile error.
- For a method on `ChainSpec`, write `spec = self`.

### Inline gate

Use an inline gate when the feature changes one value inside a larger function. The `else` branch keeps the current code.

If the value is in a loop, compute the gate one time before the loop:

```rust
let eip1234_active = spec.feature_enabled::<Eip1234>(current_epoch);
for index in indices {
    let base_reward = if let Some(on) = eip1234_active {
        features::eip1234::get_base_reward(state, index, on)?
    } else {
        state.get_base_reward(index)?
    };
}
```

### The fork epoch

If the code needs the fork epoch itself, and not the on or off answer, use `spec.feature_fork_epoch(FeatureId::Eip1234)`. It returns `None` when the feature is off. Do not read `spec.features.eip1234_fork_epoch` directly.

### Which epoch to use

- If the code processes an object, take the epoch from the object: the state, the block, the slot, or the message. Near the fork epoch, the clock and the object can be on different sides of the fork.
- If the code answers a question about now, take the epoch from the clock. Serving ranges are questions about now.

## Footprint lint

The footprint of a feature is the set of lines outside its `features/<name>/` directories that contain its type name. `make features` and `make features-check` scan all workspace `.rs` files. They skip `target`, hidden directories, the generated files, and `fresnel`.

Outside the feature directories, the type name can only appear in these forms:

- `feature_enabled::<Name>`
- `feature_fork_epoch(FeatureId::Name)`
- `#[feature_dispatch(Name => ...)]`, on one line or more
- a `use` statement, on one line or more

Any other form fails the lint, for example:

- a type annotation such as `Active<Eip1234>` or `Option<Active<Eip1234>>`.
- `FeatureId::Eip1234` outside `feature_fork_epoch(...)`.
- a comment or a doc comment that contains the type name.

The error has this form:

```text
<path>:<line>: `Eip1234` outside `features/eip1234/` must be `feature_enabled::<Eip1234>`, `feature_fork_epoch(FeatureId::Eip1234)`, `#[feature_dispatch(Eip1234 => ...)]` or an import
```

To correct a violation, move the code into the feature directory. Or write the line in one of the allowed forms.

A path into a feature directory, such as `features::eip1234::helper(...)`, is always allowed. The lint adds each line with such a path to the footprint.

### FOOTPRINT.md

`make features` writes `consensus/types/src/features/<name>/FOOTPRINT.md` when that directory exists. The file lists each allowed line and each line with a `features::<name>` path, as its path and its code. It has no line numbers, so an unrelated edit does not change it. `make features-check` fails when the file is out of date.

In a review, read `FOOTPRINT.md`. It shows each place where the feature connects to the rest of the code.

## Delete a feature

1. Run `make delete-feature FEATURE=eip1234`.
2. Fix the code by hand (see the list below).
3. Run `make features-check`.
4. Run `cargo check --workspace --tests`.
5. Remove the feature keys from each devnet config.

`make delete-feature` does these steps:

- It removes the `[eip1234]` table from `registry.toml`.
- It removes each `#[feature_dispatch(Eip1234 => ...)]` attribute. The original function body stays.
- It removes `mod eip1234;` from each parent module of a feature directory.
- It runs `git rm -r` on each `features/eip1234/` directory.
- It runs `make features`.
- It prints each line that still contains `Eip1234` or `features::eip1234`.

The printed list is not complete. It does not show unused imports or unused arguments. After step 1, fix these items by hand:

- For each inline gate, keep the `else` branch and remove the rest. Remove the gate variable, for example `let eip1234_active = ...`.
- Remove each `feature_fork_epoch(FeatureId::Eip1234)` call. Keep the code path for "feature off".
- Remove the imports of the feature: `use types::features::Eip1234;`, `use crate::features;`, and module paths such as `features::eip1234`.
- If a file has no more dispatch attributes, remove `use feature_dispatch::feature_dispatch;`.
- If only a dispatch attribute used an argument, rename the argument with a `_` prefix, for example `_slot`.
- Remove each field, argument, and accessor that holds feature data outside the feature directories.

The `demo-delete-eip8198` branch shows a full delete of EIP-8198. By hand, it removed:

- the inline gates in `attestation_rewards.rs`, `beacon_block_reward.rs`, `operation_pool/src/attestation.rs`, `process_operations.rs`, `single_pass.rs`, and `epoch_cache.rs`.
- the `previous_epoch_base_rewards` field, constructor argument, and accessor of `EpochCache`.
- the `SLOT_DURATION_SCHEDULE` check in `Config::apply_to_chain_spec`.
- `ChainSpec::configured_slot_duration_schedule`. `slot_duration_schedule` and `get_slot_duration_ms` then return the `SLOT_DURATION_MS` values.
- the unused `use feature_dispatch::feature_dispatch;` and `use crate::features;` imports.

It also renamed the unused arguments to `_slot` and `_epoch`.

## Worked example: EIP-8198

EIP-8198 lets the slot duration change at the EIP-8198 fork epoch. Rewards, penalties, and churn scale with the slot duration. The registry entry adds two optional config keys: `SLOT_DURATION_SCHEDULE` and `MIN_BLOB_DATA_RETENTION_MS`.

Feature code:

| Path | Contents |
|---|---|
| `consensus/types/src/features/eip8198/mod.rs` | `slot_duration_schedule`, `validate_slot_duration_schedule`. |
| `consensus/types/src/features/eip8198/deadlines.rs` | Deadline copies. They use the slot duration at the fork epoch. |
| `consensus/types/src/features/eip8198/rescaling.rs` | Churn limit copies, `get_base_reward_for_epoch`, `PreviousEpochBaseRewards`. |
| `consensus/types/src/features/eip8198/retention.rs` | The data retention window, fixed in milliseconds and clamped to the Fulu fork epoch. |
| `consensus/state_processing/src/features/eip8198/mod.rs` | Base reward, inactivity penalty, churn, and payload timestamp copies. |

`SlotDurationSchedule` (`consensus/types/src/core/slot_duration_schedule.rs`) and the slot clock are generic. `ChainSpec::slot_duration_schedule` returns a one-entry schedule of `SLOT_DURATION_MS` unless EIP-8198 is scheduled.

Dispatch attributes:

| Function | Epoch |
|---|---|
| `ChainSpec::get_attestation_due`, `get_payload_due`, `get_payload_attestation_due`, `get_aggregate_attestation_due`, `get_contribution_message_due`, `get_sync_message_due`, `compute_slot_component_duration_at` | The epoch of `slot`. |
| `ChainSpec::min_epoch_data_availability_boundary` | `current_epoch`, from the clock. |
| `BeaconState::get_balance_churn_limit`, `get_activation_exit_churn_limit`, `get_consolidation_churn_limit` | `self.current_epoch()`. |
| `single_pass::get_activation_exit_churn_limit`, `get_balance_churn_limit` | `state_ctxt.current_epoch`. |
| `altair::get_base_reward_per_increment` | The `epoch` argument. |
| `per_block_processing::compute_timestamp_at_slot` | The epoch of `block_slot`. |

Inline gates:

| File | Why inline |
|---|---|
| `single_pass.rs` (`process_epoch_single_pass`) | The base reward changes inside the validator loop. The gate runs one time before the loop. |
| `single_pass.rs` (`RewardsAndPenaltiesContext::new`) | One value changes: the inactivity penalty denominator. |
| `process_operations.rs` (Gloas attestations) | The base reward changes inside the attester loop. |
| `epoch_cache.rs` | The epoch cache stores `PreviousEpochBaseRewards` only when the feature is on. |
| `attestation_rewards.rs`, `beacon_block_reward.rs`, `operation_pool/src/attestation.rs` | The reward APIs and the operation pool change one value in a loop. |

`feature_fork_epoch(FeatureId::Eip8198)` calls:

- `ChainSpec::configured_slot_duration_schedule`: the schedule applies only when the feature has a fork epoch.
- `Config::apply_to_chain_spec`: it checks `SLOT_DURATION_SCHEDULE` only when the feature has a fork epoch.
- the deadline copies: they compute deadlines from the slot duration at the fork epoch.

## What Lighthouse does at the fork epoch

This code is generic. A feature does not add to it.

- `fork_version_for_epoch` and `fork_at_epoch` return the feature version from the fork epoch on. If the feature and a real fork start at the same epoch, the feature version wins.
- From the next real fork on, the fork version is the version of that real fork. The feature logic stays on.
- If two features are scheduled, the later feature version replaces the earlier one.
- `all_digest_epochs` includes the fork epoch. The node moves to new gossip topics there.
- `per_slot_processing` calls `upgrade_to_feature_fork` at the fork epoch, after the real fork upgrades. It sets `state.fork` to the feature version and changes nothing else.
- If the fork epoch is the genesis epoch, `initialize_beacon_state_from_eth1` applies the same upgrade to the genesis state.

## Errors

### Fresnel errors

Fresnel exits with code 4 when `make features-check` finds an out-of-date file. It exits with code 1 for all other errors. An error in `registry.toml` or `test_features.toml` starts with the path of that file.

| ID | Cause | Fix |
|---|---|---|
| F-R00 | `registry.toml` is not valid TOML. | Fix the TOML syntax. |
| F-R01 | The feature name is not lower case ASCII letters, digits, and `_`, or does not start with a letter. | Rename the table, for example `[eip1234]`. |
| F-R02 | `min_fork` is not a variant of `ForkName`. | Use a fork from `consensus/types/src/fork/fork_name.rs`, for example `"Heze"`. |
| F-R03 | `fork_version` has no value for a network, names an unknown network, or has a value that is not 4 bytes of `0x` hex. | Give `mainnet`, `minimal`, and `gnosis`, each as `"0x"` and 8 hex digits. |
| F-R04 | Two features use the same fork version on one network. | Use a different version. |
| F-R05 | The feature table or a `config` entry has an unknown key, a missing key, or a value of the wrong type. | Use only the fields in [Registry format](#registry-format). |
| F-R06 | Fresnel cannot find `pub enum ForkName` in `fork_name.rs`. | Make sure that the `--forks` path and the enum are correct. |
| F-R07 | Two features have the same type name, for example `eip_1234` and `eip1234`. | Rename one feature. |
| F-R08 | A config key is not upper case ASCII letters, digits, and `_`, or does not start with a letter. Two features use the same config key. A `config` entry has an empty `type`. | Correct the key name or the type. Use a key that no other feature uses. |

Other Fresnel errors:

- ``<path> is out of date. Run `make features`.``: a generated file or `FOOTPRINT.md` does not match the registry. Run `make features` and commit the result.
- ``<path>:<line>: `Eip1234` outside `features/eip1234/` must be ...``: a footprint violation (see [Footprint lint](#footprint-lint)).
- ``<registry> has no feature `eip1234` ``: `make delete-feature` got a name that is not in the registry.
- `cannot run rustfmt`, `rustfmt failed`: Fresnel formats the generated code with `rustfmt`. Install the Rust toolchain.

### Dispatch errors

The `#[feature_dispatch]` macro gives these compile errors:

- ``expected `spec` or `epoch` ``: the attribute has an unknown key.
- ``duplicate `spec` ``, ``duplicate `epoch` ``: the attribute gives a key two times.
- ``missing `spec = <expr>` ``, ``missing `epoch = <expr>` ``: the attribute has no value for the key.
- `feature_dispatch needs a name for each argument`: an argument of the function is a pattern.

### Startup errors

Lighthouse checks the feature config when it loads the network config:

- `the eip1234 fork epoch 3 is before the heze fork epoch`: the feature starts before its `min_fork`. Move the feature epoch, or schedule `min_fork` earlier.
- `the eip1234 fork needs the heze fork to be scheduled`: the feature has a fork epoch, but `min_fork` has none. Schedule `min_fork`, or remove the feature epoch.
- `experimental features cannot run on a built-in network or a shadow fork of one (genesis fork version 0x00000000)`: the config schedules a feature on mainnet, a public testnet, or a shadow fork of one. A shadow fork uses the genesis fork version of its source network. Features run only on devnets with their own genesis fork version.

When the EIP-8198 fork epoch is set, Lighthouse also checks `SLOT_DURATION_SCHEDULE`. It logs `Invalid SLOT_DURATION_SCHEDULE` with one of these errors:

- `multiple entries for epoch <epoch>`
- `slot duration <ms> at epoch <epoch> is not a positive multiple of 1000`
- `the start slot of epoch <epoch> does not fit in a u64`
- `genesis slot duration <ms> does not match SLOT_DURATION_MS <ms>`
- `the first entry must be at the genesis epoch`
- `the slot duration change at epoch <epoch> is not at the EIP-8198 fork epoch`

## Rules

- Never edit `generated.rs`, `generated_fixture.rs`, or `FOOTPRINT.md`. Change `registry.toml` and run `make features`.
- Put every behavior change behind a gate. Keep the current code as the `else` branch or the original function body.
- Put feature code and feature tests in `features/<name>/` directories.
- If the feature replaces a full function, use `#[feature_dispatch]`. If it changes one value, use an inline gate.
- Give the `Active` token as the last argument of each feature function.
- When the code processes an object, take the gate epoch from the object. Use the clock only for questions about now.
- Do not read the `spec.features` fork fields directly. Use `feature_enabled` or `feature_fork_epoch`.
- Run `make features-check` before you push.
