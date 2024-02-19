//! A helper library for parsing values from `clap::ArgMatches`.

use clap::ArgMatches;
use ssz::Decode;
use std::path::PathBuf;
use std::str::FromStr;
use types::{ChainSpec, Config, Epoch, EthSpec, Hash256};

pub mod flags;

pub struct GlobalConfig {
    pub config_file: Option<PathBuf>,
    pub spec: Option<String>,
    pub logfile: Option<PathBuf>,
    pub logfile_debug_level: String,
    pub logfile_max_size: u64,
    pub logfile_max_number: usize,
    pub logfile_compress: bool,
    pub log_format: String,
    pub debug_level: String,
    pub datadir: Option<PathBuf>,
    pub testnet_dir: Option<PathBuf>,
    pub network: Option<String>,
    pub dump_config: Option<PathBuf>,
    pub dump_chain_config: Option<PathBuf>,
    pub immediate_shutdown: bool,
    pub disable_malloc_tuning: bool,
    pub terminal_total_difficulty_override: Option<String>,
    pub terminal_block_hash_override: Option<Hash256>,
    pub terminal_block_hash_epoch_override: Option<Epoch>,
    pub genesis_state_url: Option<String>,
    pub genesis_state_url_timeout: u64,
}

/// If `name` is in `matches`, parses the value as a path. Otherwise, attempts to find the user's
/// home directory and appends `default` to it.
pub fn parse_path_with_default_in_home_dir(
    path: Option<PathBuf>,
    default: PathBuf,
) -> Result<PathBuf, String> {
    if let Some(p) = path {
        Ok(p)
    } else {
        dirs::home_dir()
            .map(|home| home.join(default))
            .ok_or_else(|| "Unable to locate home directory.".to_string())
    }
}

/// Returns the value of `name` or an error if it is not in `matches` or does not parse
/// successfully using `std::string::FromStr`.
pub fn parse_required<T>(matches: &ArgMatches, name: &str) -> Result<T, String>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    parse_optional(matches, name)?.ok_or_else(|| format!("{} not specified", name))
}

/// Returns the value of `name` (if present) or an error if it does not parse successfully using
/// `std::string::FromStr`.
pub fn parse_optional<T>(matches: &ArgMatches, name: &str) -> Result<Option<T>, String>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    matches
        .value_of(name)
        .map(|val| {
            val.parse()
                .map_err(|e| format!("Unable to parse {}: {}", name, e))
        })
        .transpose()
}

/// Returns the value of `name` or an error if it is not in `matches` or does not parse
/// successfully using `ssz::Decode`.
///
/// Expects the value of `name` to be 0x-prefixed ASCII-hex.
pub fn parse_ssz_required<T: Decode>(
    matches: &ArgMatches,
    name: &'static str,
) -> Result<T, String> {
    parse_ssz_optional(matches, name)?.ok_or_else(|| format!("{} not specified", name))
}

/// Returns the value of `name` (if present) or an error if it does not parse successfully using
/// `ssz::Decode`.
///
/// Expects the value of `name` (if any) to be 0x-prefixed ASCII-hex.
pub fn parse_ssz_optional<T: Decode>(
    matches: &ArgMatches,
    name: &'static str,
) -> Result<Option<T>, String> {
    matches
        .value_of(name)
        .map(|val| {
            if let Some(stripped) = val.strip_prefix("0x") {
                let vec = hex::decode(stripped)
                    .map_err(|e| format!("Unable to parse {} as hex: {:?}", name, e))?;

                T::from_ssz_bytes(&vec)
                    .map_err(|e| format!("Unable to parse {} as SSZ: {:?}", name, e))
            } else {
                Err(format!("Unable to parse {}, must have 0x prefix", name))
            }
        })
        .transpose()
}

pub fn dump_config<S, E>(dump_path: PathBuf, config: S) -> Result<(), String>
where
    S: serde::Serialize,
    E: EthSpec,
{
    let mut file = std::fs::File::create(dump_path)
        .map_err(|e| format!("Failed to open file for writing config: {:?}", e))?;
    serde_json::to_writer(&mut file, &config)
        .map_err(|e| format!("Error serializing config: {:?}", e))?;

    Ok(())
}

pub fn dump_chain_config<E>(dump_path: PathBuf, spec: &ChainSpec) -> Result<(), String>
where
    E: EthSpec,
{
    let chain_config = Config::from_chain_spec::<E>(spec);
    let mut file = std::fs::File::create(dump_path)
        .map_err(|e| format!("Failed to open file for writing chain config: {:?}", e))?;
    serde_yaml::to_writer(&mut file, &chain_config)
        .map_err(|e| format!("Error serializing config: {:?}", e))?;

    Ok(())
}

/// Writes configs to file if `dump-config` or `dump-chain-config` flags are set
pub fn check_dump_configs<S, E>(
    global_config: &GlobalConfig,
    config: S,
    spec: &ChainSpec,
) -> Result<(), String>
where
    S: serde::Serialize,
    E: EthSpec,
{
    if let Some(dump_path) = global_config.dump_config.as_ref() {
        let mut file = std::fs::File::create(dump_path)
            .map_err(|e| format!("Failed to open file for writing config: {:?}", e))?;
        serde_json::to_writer(&mut file, &config)
            .map_err(|e| format!("Error serializing config: {:?}", e))?;
    }
    if let Some(dump_path) = global_config.dump_chain_config.as_ref() {
        let chain_config = Config::from_chain_spec::<E>(spec);
        let mut file = std::fs::File::create(dump_path)
            .map_err(|e| format!("Failed to open file for writing chain config: {:?}", e))?;
        serde_yaml::to_writer(&mut file, &chain_config)
            .map_err(|e| format!("Error serializing config: {:?}", e))?;
    }
    Ok(())
}
