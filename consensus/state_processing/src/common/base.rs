use safe_arith::ArithError;
use types::state::base_rewards;
use types::*;

/// This type exists to avoid confusing `total_active_balance` with `sqrt_total_active_balance`,
/// since they are used in close proximity and have the same type (`u64`).
#[derive(Copy, Clone)]
pub struct SqrtTotalActiveBalance(u64);

impl SqrtTotalActiveBalance {
    pub fn new(total_active_balance: u64) -> Self {
        Self(base_rewards::integer_sqrt(total_active_balance))
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Returns the base reward for some validator.
pub fn get_base_reward(
    validator_effective_balance: u64,
    sqrt_total_active_balance: SqrtTotalActiveBalance,
    spec: &ChainSpec,
) -> Result<u64, ArithError> {
    base_rewards::phase0_base_reward(
        validator_effective_balance,
        sqrt_total_active_balance.as_u64(),
        spec.base_reward_factor,
        spec.base_rewards_per_epoch,
    )
}
