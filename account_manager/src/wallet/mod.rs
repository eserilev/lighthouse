pub mod cli;
pub mod create;
pub mod list;
pub mod recover;
use self::cli::Wallet;
use clap_utils::GlobalConfig;
use directory::{ensure_dir_exists, DEFAULT_WALLET_DIR};

pub const CMD: &str = "wallet";

pub fn cli_run(wallet_config: &Wallet, global_config: &GlobalConfig) -> Result<(), String> {
    let wallet_base_dir = if let Some(datadir) = global_config.datadir {
        datadir.join(DEFAULT_WALLET_DIR)
    } else {
        wallet_config.wallets_dir.unwrap_or(DEFAULT_WALLET_DIR)
    };
    ensure_dir_exists(&wallet_base_dir)?;

    eprintln!("wallet-dir path: {:?}", wallet_base_dir);

    match wallet_config.subcommand.as_ref() {
        cli::WalletSubcommand::Create(create_config) => {
            create::cli_run(&create_config, wallet_base_dir)
        }
        cli::WalletSubcommand::List(_) => list::cli_run(wallet_base_dir),
        cli::WalletSubcommand::Recover(recover_config) => {
            recover::cli_run(&recover_config, wallet_base_dir)
        }
    }
}
