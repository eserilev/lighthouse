use clap::{Arg, ArgAction, ArgMatches, Command};
use environment::Environment;
use slashing_protection::{
    interchange::Interchange, InterchangeError, InterchangeImportOutcome, SlashingDatabase,
    SLASHING_PROTECTION_FILENAME,
};
use std::fs::File;
use std::path::PathBuf;
use std::str::FromStr;
use types::{Epoch, EthSpec, PublicKeyBytes, Slot};

pub const CMD: &str = "slashing-protection";
pub const IMPORT_CMD: &str = "import";
pub const EXPORT_CMD: &str = "export";

pub const IMPORT_FILE_ARG: &str = "IMPORT-FILE";
pub const EXPORT_FILE_ARG: &str = "EXPORT-FILE";

pub const PUBKEYS_FLAG: &str = "pubkeys";

pub fn cli_app() -> Command {
    Command::new(CMD)
        .about("Import or export slashing protection data to or from another client")
        .subcommand(
            Command::new(IMPORT_CMD)
                .about("Import an interchange file")
                .arg(
                    Arg::new(IMPORT_FILE_ARG)
                        .action(ArgAction::Set)
                        .value_name("FILE")
                        .help("The slashing protection interchange file to import (.json)"),
                )
        )
        .subcommand(
            Command::new(EXPORT_CMD)
                .about("Export an interchange file")
                .arg(
                    Arg::new(EXPORT_FILE_ARG)
                        .action(ArgAction::Set)
                        .value_name("FILE")
                        .help("The filename to export the interchange file to"),
                )
                .arg(
                    Arg::new(PUBKEYS_FLAG)
                        .long(PUBKEYS_FLAG)
                        .action(ArgAction::Set)
                        .value_name("PUBKEYS")
                        .help(
                            "List of public keys to export history for. Keys should be 0x-prefixed, \
                             comma-separated. All known keys will be exported if omitted",
                        ),
                )
        )
}

pub fn cli_run<T: EthSpec>(
    matches: &ArgMatches,
    env: Environment<T>,
    validator_base_dir: PathBuf,
) -> Result<(), String> {
    let slashing_protection_db_path = validator_base_dir.join(SLASHING_PROTECTION_FILENAME);
    let eth2_network_config = env
        .eth2_network_config
        .ok_or("Unable to get testnet configuration from the environment")?;

    let genesis_validators_root = eth2_network_config
        .genesis_validators_root::<T>()?
        .ok_or_else(|| "Unable to get genesis state, has genesis occurred?".to_string())?;

    match matches.subcommand() {
        Some((IMPORT_CMD, matches)) => {
            let import_filename: PathBuf = clap_utils::parse_required(matches, IMPORT_FILE_ARG)?;
            let import_file = File::open(&import_filename).map_err(|e| {
                format!(
                    "Unable to open import file at {}: {:?}",
                    import_filename.display(),
                    e
                )
            })?;

            eprint!("Loading JSON file into memory & deserializing");
            let interchange = Interchange::from_json_reader(&import_file)
                .map_err(|e| format!("Error parsing file for import: {:?}", e))?;
            eprintln!(" [done].");

            let slashing_protection_database =
                SlashingDatabase::open_or_create(&slashing_protection_db_path).map_err(|e| {
                    format!(
                        "Unable to open database at {}: {:?}",
                        slashing_protection_db_path.display(),
                        e
                    )
                })?;

            let display_slot = |slot: Option<Slot>| {
                slot.map_or("none".to_string(), |slot| format!("slot {}", slot.as_u64()))
            };
            let display_epoch = |epoch: Option<Epoch>| {
                epoch.map_or("?".to_string(), |epoch| format!("epoch {}", epoch.as_u64()))
            };
            let display_attestation = |source, target| match (source, target) {
                (None, None) => "none".to_string(),
                (source, target) => {
                    format!("{} => {}", display_epoch(source), display_epoch(target))
                }
            };

            match slashing_protection_database
                .import_interchange_info(interchange, genesis_validators_root)
            {
                Ok(outcomes) => {
                    eprintln!("All records imported successfully:");
                    for outcome in &outcomes {
                        match outcome {
                            InterchangeImportOutcome::Success { pubkey, summary } => {
                                eprintln!("- {:?}", pubkey);
                                eprintln!(
                                    "    - latest proposed block: {}",
                                    display_slot(summary.max_block_slot)
                                );
                                eprintln!(
                                    "    - latest attestation: {}",
                                    display_attestation(
                                        summary.max_attestation_source,
                                        summary.max_attestation_target
                                    )
                                );
                            }
                            InterchangeImportOutcome::Failure { pubkey, error } => {
                                panic!(
                                    "import should be atomic, but key {:?} was imported despite error: {:?}",
                                    pubkey, error
                                );
                            }
                        }
                    }
                }
                Err(InterchangeError::AtomicBatchAborted(outcomes)) => {
                    eprintln!("ERROR: import aborted due to one or more errors");
                    for outcome in &outcomes {
                        if let InterchangeImportOutcome::Failure { pubkey, error } = outcome {
                            eprintln!("- {:?}", pubkey);
                            eprintln!("    - error: {:?}", error);
                        }
                    }
                    return Err("ERROR: import aborted due to errors, see above.\n\
                                No data has been imported and the slashing protection \
                                database is in the same state it was in before the import.\n\
                                Due to the failed import it is NOT SAFE to start validating\n\
                                with any newly imported validator keys, as your database lacks\n\
                                slashing protection data for them."
                        .to_string());
                }
                Err(e) => {
                    return Err(format!(
                        "Fatal error during import: {:?}\n\
                         IT IS NOT SAFE TO START VALIDATING",
                        e
                    ));
                }
            }

            eprintln!("Import completed successfully.");
            eprintln!(
                "Please double-check that the latest blocks and attestations above \
                 match your expectations."
            );

            Ok(())
        }
        Some((EXPORT_CMD, matches)) => {
            let export_filename: PathBuf = clap_utils::parse_required(matches, EXPORT_FILE_ARG)?;

            let selected_pubkeys = if let Some(pubkeys) =
                clap_utils::parse_optional::<String>(matches, PUBKEYS_FLAG)?
            {
                let pubkeys = pubkeys
                    .split(',')
                    .map(PublicKeyBytes::from_str)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| format!("Invalid --{} value: {:?}", PUBKEYS_FLAG, e))?;
                Some(pubkeys)
            } else {
                None
            };

            if !slashing_protection_db_path.exists() {
                return Err(format!(
                    "No slashing protection database exists at: {}",
                    slashing_protection_db_path.display()
                ));
            }

            let slashing_protection_database = SlashingDatabase::open(&slashing_protection_db_path)
                .map_err(|e| {
                    format!(
                        "Unable to open database at {}: {:?}",
                        slashing_protection_db_path.display(),
                        e
                    )
                })?;

            let interchange = slashing_protection_database
                .export_interchange_info(genesis_validators_root, selected_pubkeys.as_deref())
                .map_err(|e| format!("Error during export: {:?}", e))?;

            let output_file = File::create(export_filename)
                .map_err(|e| format!("Error creating output file: {:?}", e))?;

            interchange
                .write_to(&output_file)
                .map_err(|e| format!("Error writing output file: {:?}", e))?;

            eprintln!("Export completed successfully");

            Ok(())
        }
        Some((command, _)) => Err(format!("No such subcommand `{}`", command)),
        _ => Err("No subcommand provided, see --help for options".to_string()),
    }
}
