mod builder_definitions;
use builder_definitions::BuilderConfigFile;
pub use builder_definitions::{
    BUILDERS_FILENAME, BuilderDefinition, BuilderGlobals, DEFAULT_BUILDER_BOOST_FACTOR, Error,
    ResolvedBuilderConfig, ValidatorBuilderConfig, ValidatorBuilderDefinition, parse_builder_url,
    redacted_builder_url, validate_builders,
};
use builder_types::{
    BuilderConfig, BuilderEntry, BuilderPubkeys, RequestAuthData, SignedRequestAuth,
};
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use ssz_types::VariableList;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

#[derive(Clone)]
pub struct BuilderStore {
    /// The contents of the config file. Only this value is written to disk.
    config: Arc<RwLock<BuilderConfigFile>>,
    /// The process-level defaults from the CLI. They take precedence over the file globals.
    globals: Arc<BuilderGlobals>,
    validators_dir: PathBuf,
}

impl BuilderStore {
    pub fn open_or_create<P: AsRef<Path>>(validators_dir: P) -> Result<Self, Error> {
        Self::open_or_create_with_globals(validators_dir, BuilderGlobals::default())
    }

    /// Open or create the config file and apply the process-level defaults from the CLI.
    pub fn open_or_create_with_globals<P: AsRef<Path>>(
        validators_dir: P,
        globals: BuilderGlobals,
    ) -> Result<Self, Error> {
        let validators_dir = validators_dir.as_ref().to_path_buf();
        if let Some(builders) = &globals.builders {
            validate_builders(builders)?;
        }
        let config = BuilderConfigFile::open_or_create(&validators_dir)?;
        log_globals(&config, &globals);

        Ok(Self {
            config: Arc::new(RwLock::new(config)),
            globals: Arc::new(globals),
            validators_dir,
        })
    }

    /// Resolve the enabled builders into a wire [`BuilderConfig`], signing each builder's request
    /// auth via `sign`.
    ///
    /// Per-builder `min_bid`/`builder_boost_factor` inherit the global defaults when unset, and each
    /// builder's `auth_data` defaults to its lowercase ASCII hostname when unset. `sign` receives a
    /// builder's opaque auth `data` and returns the corresponding `SignedRequestAuth` — in
    /// practice signed for the current proposer/slot and cached.
    ///
    /// Signing is per-builder: a builder whose auth `sign` fails to produce is logged (with the
    /// returned error) and omitted, so one unsignable builder cannot drop the rest. The returned
    /// config always carries the validator's resolved policy; its `builders` list holds only the
    /// successfully signed builders, and is empty when no builders are enabled or every one
    /// failed to sign.
    pub async fn builder_config<F, Fut, E>(
        &self,
        validator_pubkey: &bls::PublicKeyBytes,
        sign: F,
    ) -> BuilderConfig
    where
        F: Fn(RequestAuthData) -> Fut,
        Fut: Future<Output = Result<SignedRequestAuth, E>>,
        E: std::fmt::Debug,
    {
        // Snapshot the validator's resolved builders and policy under the lock, then sign outside
        // it, so the lock is never held across an `.await`.
        let (definitions, min_bid, builder_boost_factor) = {
            let config = self.config.read();
            let resolved = config.resolved_for(validator_pubkey, &self.globals);
            (
                resolved.builders,
                resolved.min_bid,
                resolved.builder_boost_factor,
            )
        };

        // Sign every builder's request auth concurrently. With a remote signer each `sign` is a
        // network round trip, and the signatures are independent, so signing in sequence would put
        // up to `MaxBuilderEntries` serial round trips on the block-production critical path.
        let signed = futures::future::join_all(definitions.into_iter().filter_map(|definition| {
            let auth_data = match definition
                .auth_data
                .clone()
                .map(Ok)
                .unwrap_or_else(|| definition.url.to_default_auth_data())
            {
                Ok(data) => data,
                Err(error) => {
                    error!(
                        error = ?error,
                        builder_url = %definition.url,
                        "Failed to derive builder auth_data; omitting builder from config"
                    );
                    return None;
                }
            };
            // A zero-length auth `data` is invalid on the wire (beacon-specs #165 / beacon-APIs
            // #630); the beacon node would reject the whole request body, so drop the builder here.
            if auth_data.is_empty() {
                error!(
                    builder_url = %definition.url,
                    "Zero-length auth_data is invalid; omitting builder from config"
                );
                return None;
            }
            let signing = sign(auth_data);
            Some(async move { (definition, signing.await) })
        }))
        .await;

        // `join_all` preserves input order, so `builders` keeps the configured order. Omit any
        // builder we cannot sign for, logging the error, rather than failing the whole config.
        let mut builders = Vec::with_capacity(signed.len());
        for (definition, result) in signed {
            let auth = match result {
                Ok(auth) => auth,
                Err(e) => {
                    error!(
                        error = ?e,
                        builder_url = %definition.url,
                        "Failed to sign builder request auth; omitting builder from config"
                    );
                    continue;
                }
            };
            let Ok(builder_pubkeys) = BuilderPubkeys::new(definition.builder_pubkeys) else {
                error!(
                    builder_url = %definition.url,
                    "Too many builder pubkeys; omitting builder from config"
                );
                continue;
            };
            builders.push(BuilderEntry {
                url: definition.url,
                auth,
                builder_pubkeys,
                max_execution_payment: definition.max_execution_payment.unwrap_or_default(),
                min_bid: definition.min_bid.unwrap_or(min_bid),
                builder_boost_factor: definition
                    .builder_boost_factor
                    .unwrap_or(builder_boost_factor),
            });
        }

        BuilderConfig {
            // The number of builders is bounded by `MaxBuilderEntries` at config load, so this
            // cannot overflow.
            builders: VariableList::new(builders)
                .expect("builder count is bounded by MaxBuilderEntries at config load"),
            min_bid,
            builder_boost_factor,
        }
    }

    pub fn insert(&self, builder: BuilderDefinition) -> Result<(), Error> {
        self.persist_update(|candidate| {
            candidate.push(builder);
            candidate.validate()?;
            Ok(true)
        })
    }

    /// True when the global builders list in use has an enabled builder.
    pub fn has_global_builders(&self) -> bool {
        match &self.globals.builders {
            Some(builders) => !builders.is_empty(),
            None => self
                .config
                .read()
                .builders
                .iter()
                .any(|builder| builder.enabled),
        }
    }

    /// Return the fully resolved configuration for a validator without signing builder auth data.
    pub fn get_validator_config(
        &self,
        validator_pubkey: &bls::PublicKeyBytes,
    ) -> ResolvedBuilderConfig {
        self.config
            .read()
            .resolved_for(validator_pubkey, &self.globals)
    }

    /// Replace the per-validator configuration and persist it atomically.
    pub fn set_validator_config(
        &self,
        validator_pubkey: &bls::PublicKeyBytes,
        validator_config: ValidatorBuilderConfig,
    ) -> Result<(), Error> {
        validator_config.validate()?;

        self.persist_update(|candidate| {
            candidate
                .validator_configs
                .insert(validator_pubkey.to_string(), validator_config);
            Ok(true)
        })
    }

    /// Remove a validator's override and persist the inherited global configuration atomically.
    pub fn delete_validator_config(
        &self,
        validator_pubkey: &bls::PublicKeyBytes,
    ) -> Result<(), Error> {
        self.persist_update(|candidate| {
            Ok(candidate
                .validator_configs
                .remove(&validator_pubkey.to_string())
                .is_some())
        })
    }

    /// Apply and persist one update while ordinary readers continue using the current config.
    ///
    /// The update returns `false` when it made no change and no save is required.
    /// The upgradable read guard serializes writers from snapshot through save. The brief upgrade
    /// publishes only a successfully persisted candidate.
    fn persist_update(
        &self,
        update: impl FnOnce(&mut BuilderConfigFile) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        let config = self.config.upgradable_read();
        let mut candidate = config.clone();
        let changed = update(&mut candidate)?;
        if !changed {
            return Ok(());
        }
        candidate.save(&self.validators_dir)?;
        *RwLockUpgradableReadGuard::upgrade(config) = candidate;
        Ok(())
    }
}

/// A cap above this value (10 ETH, in gwei) is likely a unit error.
const MAX_EXPECTED_EXECUTION_PAYMENT: u64 = 10_000_000_000;

const GWEI_PER_ETH: u64 = 1_000_000_000;

fn gwei_to_eth(gwei: u64) -> String {
    let eth = format!("{}.{:09}", gwei / GWEI_PER_ETH, gwei % GWEI_PER_ETH);
    eth.trim_end_matches('0').trim_end_matches('.').to_string()
}

/// The file globals in use. `v8.3.0-rc.0` wrote the defaults to new files, so a default value
/// counts as unset.
fn file_globals(file: &BuilderConfigFile) -> (Option<u64>, Option<u64>) {
    (
        file.min_bid.filter(|min_bid| *min_bid != 0),
        file.builder_boost_factor
            .filter(|factor| *factor != DEFAULT_BUILDER_BOOST_FACTOR),
    )
}

fn enabled_file_builders(file: &BuilderConfigFile) -> Vec<BuilderDefinition> {
    file.builders
        .iter()
        .filter(|builder| builder.enabled)
        .cloned()
        .collect()
}

fn boost_factor_flag(globals: &BuilderGlobals) -> &'static str {
    if globals.prefer_builder_proposals {
        "--prefer-builder-proposals"
    } else {
        "--builder-boost-factor"
    }
}

/// A CLI flag that overrides a different value in the file.
#[derive(Debug, PartialEq)]
struct Override {
    flag: &'static str,
    cli_value: u64,
    file_value: u64,
}

/// The CLI flags that override a different global value in the file. For `--builder-urls`, the
/// values are the number of builders.
fn overridden_file_globals(file: &BuilderConfigFile, globals: &BuilderGlobals) -> Vec<Override> {
    let (file_min_bid, file_builder_boost_factor) = file_globals(file);
    let mut overridden = vec![];
    if let (Some(cli_value), Some(file_value)) = (globals.min_bid, file_min_bid)
        && cli_value != file_value
    {
        overridden.push(Override {
            flag: "--builder-min-bid",
            cli_value,
            file_value,
        });
    }
    if let (Some(cli_value), Some(file_value)) =
        (globals.builder_boost_factor, file_builder_boost_factor)
        && cli_value != file_value
    {
        overridden.push(Override {
            flag: boost_factor_flag(globals),
            cli_value,
            file_value,
        });
    }
    let file_builders = enabled_file_builders(file);
    if let Some(cli_builders) = &globals.builders
        && !file_builders.is_empty()
        && *cli_builders != file_builders
    {
        overridden.push(Override {
            flag: "--builder-urls",
            cli_value: cli_builders.len() as u64,
            file_value: file_builders.len() as u64,
        });
    }
    overridden
}

/// A value of a builder in the file that takes precedence over a CLI flag.
#[derive(Debug, PartialEq)]
struct BuilderOverride {
    index: usize,
    flag: &'static str,
    builder_value: u64,
    cli_value: u64,
}

/// The values of builders in the file that take precedence over CLI flags. This happens only when
/// `--builder-urls` is not set, because the file builders are then in use.
fn overriding_file_builders(
    file: &BuilderConfigFile,
    globals: &BuilderGlobals,
) -> Vec<BuilderOverride> {
    if globals.builders.is_some() {
        return vec![];
    }
    let mut overrides = vec![];
    for (index, builder) in enabled_file_builders(file).iter().enumerate() {
        let values = [
            ("--builder-min-bid", builder.min_bid, globals.min_bid),
            (
                boost_factor_flag(globals),
                builder.builder_boost_factor,
                globals.builder_boost_factor,
            ),
            (
                "--builder-max-execution-payment",
                builder.max_execution_payment,
                globals.max_execution_payment,
            ),
        ];
        for (flag, builder_value, cli_value) in values {
            if let (Some(builder_value), Some(cli_value)) = (builder_value, cli_value)
                && builder_value != cli_value
            {
                overrides.push(BuilderOverride {
                    index,
                    flag,
                    builder_value,
                    cli_value,
                });
            }
        }
    }
    overrides
}

/// Warn about each conflict between the CLI and the file, then log the effective globals.
fn log_globals(file: &BuilderConfigFile, globals: &BuilderGlobals) {
    for Override {
        flag,
        cli_value,
        file_value,
    } in overridden_file_globals(file, globals)
    {
        if flag == "--builder-urls" {
            warn!(
                flag,
                cli_builders = cli_value,
                file_builders = file_value,
                file = BUILDERS_FILENAME,
                "CLI flag replaces the builders list in the builder definitions file, including \
                 its builder_pubkeys and max_execution_payment values"
            );
        } else {
            warn!(
                flag,
                cli_value,
                file_value,
                file = BUILDERS_FILENAME,
                "CLI flag overrides a different global value in the builder definitions file"
            );
        }
    }
    for BuilderOverride {
        index,
        flag,
        builder_value,
        cli_value,
    } in overriding_file_builders(file, globals)
    {
        warn!(
            builder = index,
            flag,
            builder_value,
            cli_value,
            file = BUILDERS_FILENAME,
            "Builder value in the builder definitions file takes precedence over CLI flag"
        );
    }

    let (file_min_bid, file_builder_boost_factor) = file_globals(file);
    let file_builders = enabled_file_builders(file);
    let builders = globals.builders.as_deref().unwrap_or(&file_builders);

    if let Some(cap) = globals.max_execution_payment
        && cap > MAX_EXPECTED_EXECUTION_PAYMENT
    {
        warn!(
            max_execution_payment_gwei = cap,
            max_execution_payment_eth = gwei_to_eth(cap),
            "--builder-max-execution-payment is more than 10 ETH. Make sure that the value is in \
             Gwei"
        );
    }
    for (index, builder) in builders.iter().enumerate() {
        if let Some(cap) = builder.max_execution_payment
            && cap > MAX_EXPECTED_EXECUTION_PAYMENT
        {
            warn!(
                builder = index,
                max_execution_payment_gwei = cap,
                max_execution_payment_eth = gwei_to_eth(cap),
                "Builder max_execution_payment is more than 10 ETH. Make sure that the value is \
                 in Gwei"
            );
        }
    }

    let source = |cli: bool, file: bool| match (cli, file) {
        (true, _) => "cli",
        (false, true) => "file",
        (false, false) => "default",
    };
    let min_bid_source = source(globals.min_bid.is_some(), file_min_bid.is_some());
    let builder_boost_factor_source = source(
        globals.builder_boost_factor.is_some(),
        file_builder_boost_factor.is_some(),
    );
    let builders_source = source(globals.builders.is_some(), !file_builders.is_empty());
    if min_bid_source == "default"
        && builder_boost_factor_source == "default"
        && builders_source == "default"
        && globals.max_execution_payment.is_none()
        && file.validator_configs.is_empty()
    {
        debug!("Gloas builder configuration uses the defaults");
        return;
    }
    info!(
        min_bid = globals.min_bid.or(file_min_bid).unwrap_or_default(),
        min_bid_source,
        builder_boost_factor = globals
            .builder_boost_factor
            .or(file_builder_boost_factor)
            .unwrap_or(DEFAULT_BUILDER_BOOST_FACTOR),
        builder_boost_factor_source,
        max_execution_payment_gwei = globals.max_execution_payment,
        builders = builders.len(),
        builders_source,
        validator_overrides = file.validator_configs.len(),
        "Gloas builder configuration"
    );
    for (index, builder) in builders.iter().enumerate() {
        let cap = builder
            .max_execution_payment
            .or(globals.max_execution_payment)
            .unwrap_or_default();
        info!(
            builder = index,
            builder_url = redacted_builder_url(&builder.url),
            max_execution_payment_gwei = cap,
            max_execution_payment_eth = gwei_to_eth(cap),
            "Gloas builder"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bls::Keypair;
    use builder_types::RequestAuth;
    use tempfile::tempdir;
    use types::Slot;

    fn global_builder(url: &str, max_execution_payment: u64) -> BuilderDefinition {
        BuilderDefinition {
            enabled: true,
            url: url.parse().unwrap(),
            auth_data: None,
            builder_pubkeys: vec![],
            max_execution_payment: Some(max_execution_payment),
            min_bid: None,
            builder_boost_factor: None,
        }
    }

    fn validator_builder(url: &str) -> ValidatorBuilderDefinition {
        ValidatorBuilderDefinition {
            url: url.parse().unwrap(),
            auth_data: None,
            builder_pubkeys: vec![],
            max_execution_payment: None,
            min_bid: None,
            builder_boost_factor: None,
        }
    }

    fn signed_auth(data: RequestAuthData) -> SignedRequestAuth {
        SignedRequestAuth {
            message: RequestAuth {
                data,
                slot: Slot::new(0),
            },
            signature: bls::Signature::empty(),
        }
    }

    #[test]
    fn validator_config_inherits_global_and_distinguishes_empty_builders() {
        let directory = tempdir().unwrap();
        let store = BuilderStore::open_or_create(directory.path()).unwrap();
        let validator = Keypair::random().pk.compress();

        let mut configured_global_builder = global_builder("https://global-builder.example", 7);
        configured_global_builder.min_bid = Some(100);
        configured_global_builder.builder_boost_factor = Some(200);
        store.insert(configured_global_builder).unwrap();
        let mut empty_auth = global_builder("https://empty-auth.example", 7);
        empty_auth.auth_data = Some(RequestAuthData::default());
        store.insert(empty_auth).unwrap();
        let mut excessive_pubkeys = global_builder("https://too-many-pubkeys.example", 7);
        excessive_pubkeys.builder_pubkeys =
            (0..65).map(|_| Keypair::random().pk.compress()).collect();
        store.insert(excessive_pubkeys).unwrap();
        store
            .set_validator_config(
                &validator,
                ValidatorBuilderConfig {
                    min_bid: Some(5),
                    builder_boost_factor: Some(125),
                    builders: None,
                },
            )
            .unwrap();

        let inherited = store.get_validator_config(&validator);
        assert_eq!(inherited.min_bid, 5);
        assert_eq!(inherited.builder_boost_factor, 125);
        assert_eq!(inherited.builders.len(), 1);
        assert_eq!(inherited.builders[0].max_execution_payment, Some(7));
        assert_eq!(inherited.builders[0].min_bid, Some(5));
        assert_eq!(inherited.builders[0].builder_boost_factor, Some(125));

        store
            .set_validator_config(
                &validator,
                ValidatorBuilderConfig {
                    min_bid: None,
                    builder_boost_factor: None,
                    builders: Some(vec![]),
                },
            )
            .unwrap();
        assert!(store.get_validator_config(&validator).builders.is_empty());

        store.delete_validator_config(&validator).unwrap();
        let restored = store.get_validator_config(&validator);
        assert_eq!(restored.min_bid, 0);
        assert_eq!(restored.builder_boost_factor, 100);
        assert_eq!(restored.builders.len(), 1);
        assert_eq!(restored.builders[0].min_bid, Some(100));
        assert_eq!(restored.builders[0].builder_boost_factor, Some(200));
    }

    #[test]
    fn empty_validator_config_persists_across_store_restart() {
        let directory = tempdir().unwrap();
        let store = BuilderStore::open_or_create(directory.path()).unwrap();
        let validator = Keypair::random().pk.compress();

        store
            .set_validator_config(&validator, ValidatorBuilderConfig::default())
            .unwrap();

        let file = builder_definitions::BuilderConfigFile::open(directory.path()).unwrap();
        assert!(file.validator_configs.contains_key(&validator.to_string()));

        let restarted = BuilderStore::open_or_create(directory.path()).unwrap();
        assert_eq!(
            restarted.get_validator_config(&validator),
            store.get_validator_config(&validator)
        );

        restarted.delete_validator_config(&validator).unwrap();
        let file = builder_definitions::BuilderConfigFile::open(directory.path()).unwrap();
        assert!(!file.validator_configs.contains_key(&validator.to_string()));
    }

    #[test]
    fn failed_insert_is_not_published() {
        let directory = tempdir().unwrap();
        let store = BuilderStore::open_or_create(directory.path()).unwrap();
        let validator = Keypair::random().pk.compress();
        std::fs::create_dir(
            directory
                .path()
                .join(builder_definitions::BUILDERS_TEMP_FILENAME),
        )
        .unwrap();

        assert!(
            store
                .insert(global_builder("https://global-builder.example", 7))
                .is_err()
        );
        assert!(store.get_validator_config(&validator).builders.is_empty());
    }

    #[test]
    fn validator_updates_are_serialized_without_losing_each_other() {
        let directory = tempdir().unwrap();
        let store = Arc::new(BuilderStore::open_or_create(directory.path()).unwrap());
        let first = Keypair::random().pk.compress();
        let second = Keypair::random().pk.compress();

        let handles = [(first, 11), (second, 22)].map(|(validator, min_bid)| {
            let store = store.clone();
            std::thread::spawn(move || {
                store.set_validator_config(
                    &validator,
                    ValidatorBuilderConfig {
                        min_bid: Some(min_bid),
                        ..Default::default()
                    },
                )
            })
        });
        for handle in handles {
            handle.join().unwrap().unwrap();
        }
        assert_eq!(store.get_validator_config(&first).min_bid, 11);
        assert_eq!(store.get_validator_config(&second).min_bid, 22);

        let restarted = BuilderStore::open_or_create(directory.path()).unwrap();
        assert_eq!(restarted.get_validator_config(&first).min_bid, 11);
        assert_eq!(restarted.get_validator_config(&second).min_bid, 22);
    }

    #[test]
    fn builder_consumer_observes_runtime_updates_immediately() {
        let directory = tempdir().unwrap();
        let store = BuilderStore::open_or_create(directory.path()).unwrap();
        let validator = Keypair::random().pk.compress();
        store
            .insert(global_builder("https://global-builder.example", 7))
            .unwrap();

        store
            .set_validator_config(
                &validator,
                ValidatorBuilderConfig {
                    builders: Some(vec![]),
                    ..Default::default()
                },
            )
            .unwrap();
        let empty =
            futures::executor::block_on(store.builder_config(&validator, |data| async move {
                Ok::<_, ()>(signed_auth(data))
            }));
        assert!(empty.builders.is_empty());

        store.delete_validator_config(&validator).unwrap();
        let inherited =
            futures::executor::block_on(store.builder_config(&validator, |data| async move {
                Ok::<_, ()>(signed_auth(data))
            }));
        assert_eq!(inherited.builders.len(), 1);
        assert_eq!(inherited.builders[0].max_execution_payment, 7);
    }

    #[test]
    fn global_and_validator_auth_data_use_hostname_or_exact_override() {
        let directory = tempdir().unwrap();
        let store = BuilderStore::open_or_create(directory.path()).unwrap();
        let validator = Keypair::random().pk.compress();
        let url = "HTTPS://Builder.Example:443/path?query#fragment";
        store.insert(global_builder(url, 9)).unwrap();

        let inherited =
            futures::executor::block_on(store.builder_config(&validator, |data| async move {
                Ok::<_, ()>(signed_auth(data))
            }));
        assert_eq!(
            &*inherited.builders[0].auth.message.data,
            b"builder.example"
        );
        assert_eq!(inherited.builders[0].url.as_str().unwrap(), url);

        for explicit in [
            None,
            Some(b"builder.example".to_vec()),
            Some(vec![0xff, 0x00, b'A']),
        ] {
            let expected = explicit
                .clone()
                .unwrap_or_else(|| b"builder.example".to_vec());
            let mut builder = validator_builder(url);
            builder.auth_data = explicit.map(|bytes| RequestAuthData::new(bytes).unwrap());
            store
                .set_validator_config(
                    &validator,
                    ValidatorBuilderConfig {
                        builders: Some(vec![builder]),
                        ..Default::default()
                    },
                )
                .unwrap();
            let config =
                futures::executor::block_on(store.builder_config(&validator, |data| async move {
                    Ok::<_, ()>(signed_auth(data))
                }));
            assert_eq!(&*config.builders[0].auth.message.data, expected.as_slice());
            assert_eq!(
                config.builders[0].max_execution_payment,
                if expected == b"builder.example" { 9 } else { 0 }
            );
        }
    }

    #[test]
    fn unicode_hostname_needs_explicit_auth_data() {
        let directory = tempdir().unwrap();
        let store = BuilderStore::open_or_create(directory.path()).unwrap();
        let validator = Keypair::random().pk.compress();
        let url = "https://bücher.example/";
        assert!(matches!(
            store.insert(global_builder(url, 9)),
            Err(Error::InvalidBuilderUrl(_))
        ));
        let mut config = ValidatorBuilderConfig {
            builders: Some(vec![validator_builder(url)]),
            ..Default::default()
        };
        assert!(matches!(
            store.set_validator_config(&validator, config.clone()),
            Err(Error::InvalidBuilderUrl(_))
        ));
        let explicit = RequestAuthData::new(vec![0xff, 0x00, b'A']).unwrap();
        let mut global = global_builder(url, 9);
        global.auth_data = Some(explicit.clone());
        store.insert(global).unwrap();
        config.builders.as_mut().unwrap()[0].auth_data = Some(explicit.clone());
        store.set_validator_config(&validator, config).unwrap();
        let signed =
            futures::executor::block_on(store.builder_config(&validator, |data| async move {
                Ok::<_, ()>(signed_auth(data))
            }));
        assert_eq!(signed.builders[0].auth.message.data, explicit);
        assert_eq!(signed.builders[0].max_execution_payment, 9);
    }

    #[test]
    fn custom_builders_resolve_payment_limits_from_enabled_globals() {
        let directory = tempdir().unwrap();
        let store = BuilderStore::open_or_create(directory.path()).unwrap();
        let validator = Keypair::random().pk.compress();
        store
            .insert(global_builder("https://global-builder.example", 9))
            .unwrap();
        store
            .insert(BuilderDefinition {
                enabled: false,
                url: "https://disabled-builder.example".parse().unwrap(),
                auth_data: None,
                builder_pubkeys: vec![],
                max_execution_payment: Some(9),
                min_bid: None,
                builder_boost_factor: None,
            })
            .unwrap();

        store
            .set_validator_config(
                &validator,
                ValidatorBuilderConfig {
                    builders: Some(vec![
                        validator_builder("https://global-builder.example"),
                        validator_builder("https://disabled-builder.example"),
                    ]),
                    ..Default::default()
                },
            )
            .unwrap();

        let resolved = store.get_validator_config(&validator);
        assert_eq!(resolved.builders[0].max_execution_payment, Some(9));
        assert_eq!(resolved.builders[1].max_execution_payment, Some(0));
    }

    fn write_file(directory: &Path, file: builder_definitions::BuilderConfigFile) {
        file.save(directory).unwrap();
    }

    #[test]
    fn new_file_does_not_write_globals() {
        let directory = tempdir().unwrap();
        BuilderStore::open_or_create(directory.path()).unwrap();
        let contents = std::fs::read_to_string(directory.path().join(BUILDERS_FILENAME)).unwrap();
        for field in ["min_bid", "builder_boost_factor", "builders"] {
            assert!(
                !contents.contains(field),
                "unexpected `{field}`:\n{contents}"
            );
        }
    }

    #[test]
    fn cli_globals_override_file_globals_per_field() {
        let directory = tempdir().unwrap();
        write_file(
            directory.path(),
            builder_definitions::BuilderConfigFile {
                min_bid: Some(7),
                builder_boost_factor: Some(150),
                builders: vec![global_builder("https://file.example", 5)],
                validator_configs: Default::default(),
            },
        );
        let store = BuilderStore::open_or_create_with_globals(
            directory.path(),
            BuilderGlobals {
                min_bid: Some(9),
                ..Default::default()
            },
        )
        .unwrap();

        let resolved = store.get_validator_config(&Keypair::random().pk.compress());
        assert_eq!(resolved.min_bid, 9);
        assert_eq!(resolved.builder_boost_factor, 150);
        assert_eq!(resolved.builders.len(), 1);
        assert_eq!(
            resolved.builders[0].url,
            "https://file.example".parse().unwrap()
        );
        assert_eq!(resolved.builders[0].min_bid, Some(9));
    }

    #[test]
    fn cli_builders_replace_file_builders_and_supply_inherited_caps() {
        let directory = tempdir().unwrap();
        write_file(
            directory.path(),
            builder_definitions::BuilderConfigFile {
                min_bid: None,
                builder_boost_factor: None,
                builders: vec![global_builder("https://file.example", 5)],
                validator_configs: Default::default(),
            },
        );
        let store = BuilderStore::open_or_create_with_globals(
            directory.path(),
            BuilderGlobals {
                builders: Some(vec![global_builder("https://cli.example", 3)]),
                ..Default::default()
            },
        )
        .unwrap();

        let inherited = store.get_validator_config(&Keypair::random().pk.compress());
        assert_eq!(inherited.builders.len(), 1);
        assert_eq!(
            inherited.builders[0].url,
            "https://cli.example".parse().unwrap()
        );
        assert_eq!(inherited.builders[0].max_execution_payment, Some(3));

        let validator = Keypair::random().pk.compress();
        store
            .set_validator_config(
                &validator,
                ValidatorBuilderConfig {
                    builders: Some(vec![
                        validator_builder("https://cli.example"),
                        validator_builder("https://file.example"),
                    ]),
                    ..Default::default()
                },
            )
            .unwrap();
        let custom = store.get_validator_config(&validator);
        assert_eq!(custom.builders[0].max_execution_payment, Some(3));
        assert_eq!(custom.builders[1].max_execution_payment, Some(0));
    }

    #[test]
    fn validator_updates_do_not_persist_cli_globals() {
        let directory = tempdir().unwrap();
        let store = BuilderStore::open_or_create_with_globals(
            directory.path(),
            BuilderGlobals {
                min_bid: Some(9),
                builder_boost_factor: Some(120),
                max_execution_payment: Some(3),
                builders: Some(vec![global_builder("https://cli.example", 3)]),
                ..Default::default()
            },
        )
        .unwrap();
        store
            .set_validator_config(
                &Keypair::random().pk.compress(),
                ValidatorBuilderConfig {
                    min_bid: Some(1),
                    ..Default::default()
                },
            )
            .unwrap();

        let file = builder_definitions::BuilderConfigFile::open(directory.path()).unwrap();
        assert_eq!(file.min_bid, None);
        assert_eq!(file.builder_boost_factor, None);
        assert!(file.builders.is_empty());
        assert_eq!(file.validator_configs.len(), 1);
    }

    #[test]
    fn invalid_cli_builders_are_rejected() {
        let directory = tempdir().unwrap();
        let result = BuilderStore::open_or_create_with_globals(
            directory.path(),
            BuilderGlobals {
                builders: Some(vec![
                    global_builder("https://cli.example", 0),
                    global_builder("https://cli.example", 0),
                ]),
                ..Default::default()
            },
        );
        assert!(matches!(result, Err(Error::DuplicateBuilderAuth(_))));
    }

    #[test]
    fn parse_builder_url_reads_auth_data_fragment() {
        let (url, auth_data) = parse_builder_url("https://builder.example").unwrap();
        assert_eq!(url, "https://builder.example".parse().unwrap());
        assert_eq!(auth_data, None);

        let (url, auth_data) = parse_builder_url("https://builder.example/path#0x0123").unwrap();
        assert_eq!(url, "https://builder.example/path".parse().unwrap());
        assert_eq!(
            auth_data,
            Some(RequestAuthData::new(vec![0x01, 0x23]).unwrap())
        );

        for invalid in [
            "https://builder.example#0123",
            "https://builder.example#0xzz",
            "https://builder.example#0x",
        ] {
            assert!(parse_builder_url(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn overridden_file_globals_ignore_defaults_and_equal_values() {
        let file =
            |min_bid, builder_boost_factor, builders| builder_definitions::BuilderConfigFile {
                min_bid,
                builder_boost_factor,
                builders,
                validator_configs: Default::default(),
            };
        let globals = BuilderGlobals {
            min_bid: Some(9),
            builder_boost_factor: Some(120),
            builders: Some(vec![global_builder("https://cli.example", 0)]),
            ..Default::default()
        };

        // The defaults that `v8.3.0-rc.0` wrote to new files.
        let rc_default = file(Some(0), Some(DEFAULT_BUILDER_BOOST_FACTOR), vec![]);
        assert!(overridden_file_globals(&rc_default, &globals).is_empty());

        let equal = file(
            Some(9),
            Some(120),
            vec![global_builder("https://cli.example", 0)],
        );
        assert!(overridden_file_globals(&equal, &globals).is_empty());

        let different = file(
            Some(7),
            Some(150),
            vec![global_builder("https://file.example", 0)],
        );
        assert_eq!(
            overridden_file_globals(&different, &globals),
            vec![
                Override {
                    flag: "--builder-min-bid",
                    cli_value: 9,
                    file_value: 7,
                },
                Override {
                    flag: "--builder-boost-factor",
                    cli_value: 120,
                    file_value: 150,
                },
                Override {
                    flag: "--builder-urls",
                    cli_value: 1,
                    file_value: 1,
                },
            ]
        );
        assert!(overridden_file_globals(&different, &BuilderGlobals::default()).is_empty());

        // A file with only disabled builders supplies no builders.
        let mut disabled = global_builder("https://file.example", 0);
        disabled.enabled = false;
        let only_disabled = file(None, None, vec![disabled]);
        assert!(overridden_file_globals(&only_disabled, &globals).is_empty());

        let prefer = BuilderGlobals {
            builder_boost_factor: Some(u64::MAX),
            prefer_builder_proposals: true,
            ..Default::default()
        };
        assert_eq!(
            overridden_file_globals(&different, &prefer)[0].flag,
            "--prefer-builder-proposals"
        );
    }

    #[test]
    fn file_builder_values_that_beat_cli_flags_are_reported() {
        let mut builder = global_builder("https://file.example", 5);
        builder.min_bid = Some(0);
        builder.builder_boost_factor = Some(120);
        let file = builder_definitions::BuilderConfigFile {
            min_bid: None,
            builder_boost_factor: None,
            builders: vec![builder],
            validator_configs: Default::default(),
        };
        let globals = BuilderGlobals {
            min_bid: Some(1),
            builder_boost_factor: Some(120),
            max_execution_payment: Some(7),
            ..Default::default()
        };
        assert_eq!(
            overriding_file_builders(&file, &globals),
            vec![
                BuilderOverride {
                    index: 0,
                    flag: "--builder-min-bid",
                    builder_value: 0,
                    cli_value: 1,
                },
                BuilderOverride {
                    index: 0,
                    flag: "--builder-max-execution-payment",
                    builder_value: 5,
                    cli_value: 7,
                },
            ]
        );

        // With `--builder-urls`, the file builders are not in use.
        let with_urls = BuilderGlobals {
            builders: Some(vec![]),
            ..globals
        };
        assert!(overriding_file_builders(&file, &with_urls).is_empty());
    }

    #[test]
    fn file_builder_without_cap_inherits_the_cli_cap() {
        let directory = tempdir().unwrap();
        let mut uncapped = global_builder("https://uncapped.example", 0);
        uncapped.max_execution_payment = None;
        write_file(
            directory.path(),
            builder_definitions::BuilderConfigFile {
                min_bid: None,
                builder_boost_factor: None,
                builders: vec![uncapped, global_builder("https://capped.example", 5)],
                validator_configs: Default::default(),
            },
        );
        let validator = Keypair::random().pk.compress();
        let caps = |globals| {
            let store =
                BuilderStore::open_or_create_with_globals(directory.path(), globals).unwrap();
            store
                .get_validator_config(&validator)
                .builders
                .iter()
                .map(|builder| builder.max_execution_payment)
                .collect::<Vec<_>>()
        };

        assert_eq!(caps(BuilderGlobals::default()), vec![Some(0), Some(5)]);
        assert_eq!(
            caps(BuilderGlobals {
                max_execution_payment: Some(7),
                ..Default::default()
            }),
            vec![Some(7), Some(5)]
        );
    }

    #[test]
    fn has_global_builders_uses_the_builders_in_use() {
        let directory = tempdir().unwrap();
        let mut disabled = global_builder("https://disabled.example", 1);
        disabled.enabled = false;
        write_file(
            directory.path(),
            builder_definitions::BuilderConfigFile {
                min_bid: None,
                builder_boost_factor: None,
                builders: vec![disabled],
                validator_configs: Default::default(),
            },
        );
        let has_builders = |globals| {
            BuilderStore::open_or_create_with_globals(directory.path(), globals)
                .unwrap()
                .has_global_builders()
        };

        assert!(!has_builders(BuilderGlobals::default()));
        assert!(has_builders(BuilderGlobals {
            builders: Some(vec![global_builder("https://cli.example", 1)]),
            ..Default::default()
        }));
    }

    #[test]
    fn validator_builders_inherit_the_cli_max_execution_payment() {
        let directory = tempdir().unwrap();
        write_file(
            directory.path(),
            builder_definitions::BuilderConfigFile {
                min_bid: None,
                builder_boost_factor: None,
                builders: vec![global_builder("https://file.example", 5)],
                validator_configs: Default::default(),
            },
        );
        let store = BuilderStore::open_or_create_with_globals(
            directory.path(),
            BuilderGlobals {
                max_execution_payment: Some(7),
                ..Default::default()
            },
        )
        .unwrap();
        let validator = Keypair::random().pk.compress();
        let mut own_cap = validator_builder("https://own.example");
        own_cap.max_execution_payment = Some(3);
        store
            .set_validator_config(
                &validator,
                ValidatorBuilderConfig {
                    builders: Some(vec![
                        own_cap,
                        validator_builder("https://file.example"),
                        validator_builder("https://other.example"),
                    ]),
                    ..Default::default()
                },
            )
            .unwrap();

        let resolved = store.get_validator_config(&validator);
        let caps: Vec<_> = resolved
            .builders
            .iter()
            .map(|builder| builder.max_execution_payment)
            .collect();
        assert_eq!(caps, vec![Some(3), Some(5), Some(7)]);
    }

    #[test]
    fn error_display_hides_url_credentials() {
        let url: builder_types::BuilderUrl =
            "https://user:secret@builder.example/token".parse().unwrap();
        for error in [
            Error::DuplicateBuilderAuth(url.clone()),
            Error::InvalidBuilderUrl(url.clone()),
            Error::UnsupportedUrlScheme(url.clone()),
            Error::TooManyBuilderPubkeys(url.clone()),
            Error::EmptyAuthData(url),
        ] {
            let message = error.to_string();
            assert!(message.contains("builder.example"), "{message}");
            assert!(!message.contains("secret"), "{message}");
            assert!(!message.contains("token"), "{message}");
        }
    }

    #[test]
    fn gwei_to_eth_formats_without_trailing_zeros() {
        assert_eq!(gwei_to_eth(1_000_000_000), "1");
        assert_eq!(gwei_to_eth(1_500_000_000), "1.5");
        assert_eq!(gwei_to_eth(1), "0.000000001");
        assert_eq!(gwei_to_eth(0), "0");
    }

    #[test]
    fn parse_builder_url_trims_whitespace() {
        let (url, _) = parse_builder_url(" https://builder.example ").unwrap();
        assert_eq!(url, "https://builder.example".parse().unwrap());

        let (url, auth_data) = parse_builder_url("https://builder.example # 0x01 ").unwrap();
        assert_eq!(url, "https://builder.example".parse().unwrap());
        assert_eq!(auth_data, Some(RequestAuthData::new(vec![0x01]).unwrap()));

        assert!(parse_builder_url("not a url").is_err());
    }
}
