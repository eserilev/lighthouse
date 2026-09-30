---
name: fresnel
description: Add, change, or delete an experimental consensus feature in Lighthouse with the Fresnel generator (consensus/types/src/features/registry.toml). Use for requests to add an experimental feature or EIP behind a feature fork, add feature config keys, gate logic with feature_enabled or #[feature_dispatch], schedule a feature on a devnet, delete a feature with make delete-feature, fix a footprint lint error or an out-of-date FOOTPRINT.md, or fix a Fresnel error ID (F-R00 to F-R08) or a feature startup error.
---

# Fresnel

Read `.ai/FRESNEL.md` before you start. It has the registry format, the gate forms, the lint rules, and the error tables.

## Add a feature

1. Add the entry to `consensus/types/src/features/registry.toml`.
2. If the feature needs config values, add them as `config` entries. If a value has no default, set `optional = true`.
3. Run `make features`.
4. Put the feature code in `consensus/types/src/features/<name>/` or `consensus/state_processing/src/features/<name>/`. Use the crate that owns the code that the feature changes.
5. Declare each new directory as `pub mod <name>;` in its parent `features/mod.rs`.
6. Give each feature function the `Active<Name>` token as its last argument.
7. If the feature replaces a full function, add `#[feature_dispatch(Name => path, spec = ..., epoch = ...)]` to the function.
8. If the feature changes one value in a larger function, add an inline `feature_enabled::<Name>(epoch)` gate. Keep the current code in the `else` branch.
9. If the value is in a loop, compute the gate one time before the loop.
10. Put the feature tests in the feature directory.
11. Run `make features`. Commit `FOOTPRINT.md` and the generated files with the code.

## Choose the gate epoch

- If the code processes a state, block, slot, or message, take the epoch from that object.
- If the code answers a question about now, take the epoch from the slot clock. A serving range is a question about now.
- If the code needs the fork epoch itself, call `spec.feature_fork_epoch(FeatureId::Name)`.

## Delete a feature

1. Run `make delete-feature FEATURE=<name>`.
2. For each inline gate that remains, keep the `else` branch and remove the rest.
3. Remove each `feature_fork_epoch(FeatureId::Name)` call. Keep the code path for "feature off".
4. Remove the feature imports and the `features::<name>` paths.
5. If a file has no more dispatch attributes, remove `use feature_dispatch::feature_dispatch;`.
6. If an argument is now unused, add a `_` prefix to its name.
7. Remove feature data outside the feature directories, for example fields and accessors.
8. Use the diff of the `demo-delete-eip8198` branch as the model.

## Fix an error

- For a footprint violation, move the line into the feature directory. Or write it as `feature_enabled::<Name>`, `feature_fork_epoch(FeatureId::Name)`, `#[feature_dispatch(Name => ...)]`, or an import.
- For an out-of-date file, run `make features` and commit the result.
- For a Fresnel error ID, a dispatch error, or a startup error, find the error in `.ai/FRESNEL.md`. Then apply its fix.

## Rules

- Never edit `generated.rs`, `generated_fixture.rs`, or `FOOTPRINT.md`.
- Never put the feature type outside a feature directory in a form that the lint rejects. This includes comments.
- Do not read the `spec.features` fork fields in logic.
- If a feature needs new fields in a consensus container, stop and tell the user. Fresnel does not support fields.

## Done when

- `make features-check` passes.
- `cargo check --workspace --tests` passes.
- Each gate keeps the current code in its `else` branch or in the original function body.
