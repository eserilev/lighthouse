//! Experimental consensus features.
//!
//! Features are listed in `registry.toml`. `make features` generates `generated.rs` from it, and
//! `generated_fixture.rs` from it and `fresnel/tests/fixtures/test_features.toml`. The
//! `fresnel-fixture` feature compiles `generated_fixture.rs` instead, so that tests have features
//! to schedule.
//!
//! Feature logic is hand-written behind a gate:
//!
//! ```ignore
//! if let Some(on) = spec.feature_enabled::<Eip1234>(epoch) {
//!     // feature code
//! }
//! ```

#[cfg_attr(feature = "fresnel-fixture", path = "generated_fixture.rs")]
mod generated;

use std::marker::PhantomData;

use crate::core::{ChainSpec, Epoch};
use crate::fork::ForkName;

pub use generated::*;

/// An experimental feature from the registry.
pub trait Feature: 'static {
    const ID: FeatureId;
    /// The first fork that the feature can run on.
    const MIN_FORK: ForkName;
}

/// Proof that feature `F` is active. Only `ChainSpec::feature_enabled` creates it.
pub struct Active<F: Feature>(PhantomData<F>);

impl<F: Feature> Clone for Active<F> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<F: Feature> Copy for Active<F> {}

impl<F: Feature> std::fmt::Debug for Active<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Active<{}>", F::ID.name())
    }
}

/// `serde_utils::bytes_4_hex` for a fork version that a config can omit.
#[allow(dead_code, reason = "used by the generated feature config")]
mod optional_fork_version {
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        version: &Option<[u8; 4]>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match version {
            Some(version) => serde_utils::bytes_4_hex::serialize(version, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<[u8; 4]>, D::Error> {
        serde_utils::bytes_4_hex::deserialize(deserializer).map(Some)
    }
}

impl ChainSpec {
    /// The fork epoch of the feature, or `None` if the feature never activates.
    pub fn feature_fork_epoch(&self, id: FeatureId) -> Option<Epoch> {
        self.features
            .fork_epoch(id)
            .filter(|epoch| *epoch != self.far_future_epoch)
    }

    /// Returns a token if feature `F` is active at `epoch`.
    pub fn feature_enabled<F: Feature>(&self, epoch: Epoch) -> Option<Active<F>> {
        let fork_epoch = self.feature_fork_epoch(F::ID)?;
        (epoch >= fork_epoch && self.fork_name_at_epoch(epoch) >= F::MIN_FORK)
            .then_some(Active(PhantomData))
    }

    /// Returns `true` if one or more features have a fork epoch.
    pub fn features_enabled(&self) -> bool {
        FeatureId::ALL
            .iter()
            .any(|id| self.feature_fork_epoch(*id).is_some())
    }

    /// Scheduled features, sorted by fork epoch, then by registry order.
    pub fn scheduled_features(&self) -> Vec<(FeatureId, Epoch)> {
        let mut scheduled: Vec<(FeatureId, Epoch)> = FeatureId::ALL
            .iter()
            .filter_map(|id| Some((*id, self.feature_fork_epoch(*id)?)))
            .collect();
        scheduled.sort_by_key(|(id, epoch)| (*epoch, *id));
        scheduled
    }

    /// Set the feature fork epochs and the fork versions that `config` has.
    pub fn with_feature_config(mut self, config: &FeatureConfig) -> Self {
        config.apply_to(&mut self.features);
        self
    }

    /// Check that each scheduled feature activates at or after the epoch of its `MIN_FORK`.
    pub fn validate_features(&self) -> Result<(), String> {
        for (id, epoch) in self.scheduled_features() {
            let min_fork = id.min_fork();
            match self.fork_epoch(min_fork) {
                Some(min_fork_epoch) if epoch >= min_fork_epoch => {}
                Some(_) => {
                    return Err(format!(
                        "the {} fork epoch {epoch} is before the {min_fork} fork epoch",
                        id.name()
                    ));
                }
                None => {
                    return Err(format!(
                        "the {} fork needs the {min_fork} fork to be scheduled",
                        id.name()
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_utils::quoted_u64::MaybeQuoted;

    fn spec() -> ChainSpec {
        let mut spec = ForkName::Gloas.make_genesis_spec(ChainSpec::mainnet());
        spec.heze_fork_epoch = Some(Epoch::new(5));
        spec
    }

    #[test]
    fn feature_gate() {
        let mut spec = spec();
        assert!(!spec.features_enabled());
        assert!(
            spec.feature_enabled::<HezeTestFeature>(Epoch::new(10))
                .is_none()
        );

        spec.features.heze_test_feature_fork_epoch = Some(Epoch::new(10));
        assert!(spec.features_enabled());
        assert!(
            spec.feature_enabled::<HezeTestFeature>(Epoch::new(9))
                .is_none()
        );
        assert!(
            spec.feature_enabled::<HezeTestFeature>(Epoch::new(10))
                .is_some()
        );

        spec.features.heze_test_feature_fork_epoch = Some(spec.far_future_epoch);
        assert!(!spec.features_enabled());
        assert!(spec.scheduled_features().is_empty());
    }

    #[test]
    fn scheduled_features_are_sorted_by_epoch_then_registry_order() {
        let mut spec = spec();
        spec.features.heze_test_feature_fork_epoch = Some(Epoch::new(5));
        spec.features.gloas_test_feature_fork_epoch = Some(Epoch::new(5));
        assert_eq!(
            spec.scheduled_features(),
            vec![
                (FeatureId::HezeTestFeature, Epoch::new(5)),
                (FeatureId::GloasTestFeature, Epoch::new(5)),
            ]
        );

        spec.features.gloas_test_feature_fork_epoch = Some(Epoch::new(1));
        assert_eq!(
            spec.scheduled_features(),
            vec![
                (FeatureId::GloasTestFeature, Epoch::new(1)),
                (FeatureId::HezeTestFeature, Epoch::new(5)),
            ]
        );
    }

    #[test]
    fn validate_features_needs_the_min_fork() {
        let mut spec = spec();
        spec.features.heze_test_feature_fork_epoch = Some(Epoch::new(5));
        assert_eq!(spec.validate_features(), Ok(()));

        spec.features.heze_test_feature_fork_epoch = Some(Epoch::new(4));
        let error = spec
            .validate_features()
            .expect_err("feature before its min fork");
        assert!(error.contains("is before the"), "{error}");

        spec.heze_fork_epoch = None;
        let error = spec
            .validate_features()
            .expect_err("feature without its min fork");
        assert!(error.contains("needs the"), "{error}");
    }

    #[test]
    fn with_feature_config_keeps_fork_versions_that_the_config_omits() {
        let spec = spec();
        let mut config = FeatureConfig::from_spec(&spec.features);
        config.heze_test_feature_fork_version = None;
        config.heze_test_feature_fork_epoch = Some(MaybeQuoted {
            value: Epoch::new(7),
        });
        config.gloas_test_feature_fork_version = Some([1, 2, 3, 4]);

        let spec = spec.with_feature_config(&config);

        assert_eq!(
            spec.features.heze_test_feature_fork_version,
            FeatureSpec::mainnet().heze_test_feature_fork_version
        );
        assert_eq!(
            spec.features.heze_test_feature_fork_epoch,
            Some(Epoch::new(7))
        );
        assert_eq!(spec.features.gloas_test_feature_fork_version, [1, 2, 3, 4]);
    }

    #[test]
    fn with_feature_config_sets_only_the_config_values_that_the_config_has() {
        let mut spec = spec();
        spec.features.heze_test_feature_limit = 3;

        let config: FeatureConfig = yaml_serde::from_str("HEZE_TEST_FEATURE_FORK_EPOCH: 5")
            .expect("config without the limit");
        assert_eq!(config.heze_test_feature_limit, None);
        let spec = spec.with_feature_config(&config);
        assert_eq!(spec.features.heze_test_feature_limit, 3);

        let config: FeatureConfig =
            yaml_serde::from_str("HEZE_TEST_FEATURE_LIMIT: 7").expect("config with the limit");
        let spec = spec.with_feature_config(&config);
        assert_eq!(spec.features.heze_test_feature_limit, 7);

        let yaml = yaml_serde::to_string(&FeatureConfig::from_spec(&spec.features))
            .expect("serialize the config");
        assert!(yaml.contains("HEZE_TEST_FEATURE_LIMIT: 7"), "{yaml}");
    }

    #[test]
    fn optional_config_values_stay_unset_until_a_config_sets_them() {
        let spec = spec();
        assert_eq!(spec.features.heze_test_feature_cap, None);
        let yaml = yaml_serde::to_string(&FeatureConfig::from_spec(&spec.features))
            .expect("serialize the config");
        assert!(!yaml.contains("HEZE_TEST_FEATURE_CAP"), "{yaml}");

        let config: FeatureConfig =
            yaml_serde::from_str("HEZE_TEST_FEATURE_CAP: 7").expect("config with the cap");
        let spec = spec.with_feature_config(&config);
        assert_eq!(spec.features.heze_test_feature_cap, Some(7));

        let config: FeatureConfig = yaml_serde::from_str("HEZE_TEST_FEATURE_FORK_EPOCH: 5")
            .expect("config without the cap");
        let spec = spec.with_feature_config(&config);
        assert_eq!(spec.features.heze_test_feature_cap, Some(7));

        let yaml = yaml_serde::to_string(&FeatureConfig::from_spec(&spec.features))
            .expect("serialize the config");
        assert!(yaml.contains("HEZE_TEST_FEATURE_CAP: 7"), "{yaml}");
    }
}
