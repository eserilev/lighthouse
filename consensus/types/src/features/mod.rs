//! Experimental consensus features.
//!
//! Features are listed in `registry.toml`. `make features` generates `generated.rs` from it.
//! Feature logic is hand-written behind a gate:
//!
//! ```ignore
//! if let Some(on) = spec.feature_enabled::<Eip1234>(epoch) {
//!     // feature code
//! }
//! ```

mod generated;

use std::marker::PhantomData;

use crate::core::{ChainSpec, Epoch};
use crate::fork::ForkName;

pub use generated::*;

/// An experimental feature from the registry.
pub trait Feature: 'static {
    const ID: FeatureId;
    const NAME: &'static str;
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
        write!(f, "Active<{}>", F::NAME)
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

    /// Set the feature fork versions and epochs from `config`, then validate them.
    pub fn apply_feature_config(mut self, config: &FeatureConfig) -> Result<Self, String> {
        self.features = config.to_spec();
        self.validate_features()?;
        Ok(self)
    }

    /// Check that each scheduled feature activates at or after the epoch of its `MIN_FORK`.
    pub fn validate_features(&self) -> Result<(), String> {
        for (id, epoch) in self.scheduled_features() {
            let min_fork = id.min_fork();
            match self.fork_epoch(min_fork) {
                Some(min_fork_epoch) if epoch >= min_fork_epoch => {}
                _ => {
                    return Err(format!(
                        "the {} fork epoch {epoch} is before the {min_fork} fork epoch",
                        id.name()
                    ));
                }
            }
        }
        Ok(())
    }
}
