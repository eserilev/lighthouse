mod common;
pub mod validator;
pub mod wallet;

use clap::{Parser, Subcommand};
use clap_utils::GlobalConfig;
use environment::Environment;
use serde::{Deserialize, Serialize};
use types::EthSpec;

pub const CMD: &str = "account_manager";
pub const SECRETS_DIR_FLAG: &str = "secrets-dir";
pub const VALIDATOR_DIR_FLAG: &str = "validator-dir";
pub const VALIDATOR_DIR_FLAG_ALIAS: &str = "validators-dir";
pub const WALLETS_DIR_FLAG: &str = "wallets-dir";

#[derive(Subcommand, Clone, Deserialize, Serialize, Debug)]
#[clap(rename_all = "snake_case", visible_aliases = &["a", "am", "account"],
about = "Utilities for generating and managing Ethereum 2.0 accounts.")]
pub enum AccountManager {
    Wallet(wallet::cli::Wallet),
    Validator(validator::cli::Validator),
}
/// Run the account manager, returning an error if the operation did not succeed.
pub fn run<T: EthSpec>(
    account_manager: &AccountManager,
    global_config: &GlobalConfig,
    env: Environment<T>,
) -> Result<(), String> {
    match account_manager {
        AccountManager::Wallet(wallet_config) => wallet::cli_run(&wallet_config, global_config)?,
        AccountManager::Validator(validator_config) => {
            validator::cli_run(&validator_config, global_config, env)?
        }
    }

    Ok(())
}
