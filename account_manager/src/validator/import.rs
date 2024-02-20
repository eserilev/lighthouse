use crate::wallet::create::{PASSWORD_FLAG, STDIN_INPUTS_FLAG};
use account_utils::validator_definitions::SigningDefinition;
use account_utils::{
    eth2_keystore::Keystore,
    read_password_from_user,
    validator_definitions::{
        recursively_find_voting_keystores, PasswordStorage, ValidatorDefinition,
        ValidatorDefinitions, CONFIG_FILENAME,
    },
    ZeroizeString,
};
use clap::{Arg, ArgAction, ArgMatches, Command};
use slashing_protection::{SlashingDatabase, SLASHING_PROTECTION_FILENAME};
use std::fs;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;

pub const CMD: &str = "import";
pub const KEYSTORE_FLAG: &str = "keystore";
pub const DIR_FLAG: &str = "directory";
pub const REUSE_PASSWORD_FLAG: &str = "reuse-password";

pub const PASSWORD_PROMPT: &str = "Enter the keystore password, or press enter to omit it:";
pub const KEYSTORE_REUSE_WARNING: &str = "DO NOT USE THE ORIGINAL KEYSTORES TO VALIDATE WITH \
                                          ANOTHER CLIENT, OR YOU WILL GET SLASHED.";

pub fn cli_app() -> Command {
    Command::new(CMD)
        .about(
            "Imports one or more EIP-2335 passwords into a Lighthouse VC directory, \
            requesting passwords interactively. The directory flag provides a convenient \
            method for importing a directory of keys generated by the eth2-deposit-cli \
            Python utility.",
        )
        .arg(
            Arg::new(KEYSTORE_FLAG)
                .long(KEYSTORE_FLAG)
                .value_name("KEYSTORE_PATH")
                .help("Path to a single keystore to be imported.")
                .conflicts_with(DIR_FLAG)
                .required_unless_present(DIR_FLAG)
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new(DIR_FLAG)
                .long(DIR_FLAG)
                .value_name("KEYSTORES_DIRECTORY")
                .help(
                    "Path to a directory which contains zero or more keystores \
                    for import. This directory and all sub-directories will be \
                    searched and any file name which contains 'keystore' and \
                    has the '.json' extension will be attempted to be imported.",
                )
                .conflicts_with(KEYSTORE_FLAG)
                .required_unless_present(KEYSTORE_FLAG)
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new(STDIN_INPUTS_FLAG)
                .action(ArgAction::SetTrue)
                .hide(cfg!(windows))
                .long(STDIN_INPUTS_FLAG)
                .help("If present, read all user inputs from stdin instead of tty."),
        )
        .arg(
            Arg::new(REUSE_PASSWORD_FLAG)
                .long(REUSE_PASSWORD_FLAG)
                .action(ArgAction::SetTrue)
                .help("If present, the same password will be used for all imported keystores."),
        )
        .arg(
            Arg::new(PASSWORD_FLAG)
                .long(PASSWORD_FLAG)
                .value_name("KEYSTORE_PASSWORD_PATH")
                .requires(REUSE_PASSWORD_FLAG)
                .help(
                    "The path to the file containing the password which will unlock all \
                    keystores being imported. This flag must be used with `--reuse-password`. \
                    The password will be copied to the `validator_definitions.yml` file, so after \
                    import we strongly recommend you delete the file at KEYSTORE_PASSWORD_PATH.",
                )
                .action(ArgAction::Set),
        )
}

pub fn cli_run(matches: &ArgMatches, validator_dir: PathBuf) -> Result<(), String> {
    let keystore: Option<PathBuf> = clap_utils::parse_optional(matches, KEYSTORE_FLAG)?;
    let keystores_dir: Option<PathBuf> = clap_utils::parse_optional(matches, DIR_FLAG)?;
    let stdin_inputs = cfg!(windows) || matches.get_flag(STDIN_INPUTS_FLAG);
    let reuse_password = matches.get_flag(REUSE_PASSWORD_FLAG);
    let keystore_password_path: Option<PathBuf> =
        clap_utils::parse_optional(matches, PASSWORD_FLAG)?;

    let mut defs = ValidatorDefinitions::open_or_create(&validator_dir)
        .map_err(|e| format!("Unable to open {}: {:?}", CONFIG_FILENAME, e))?;

    let slashing_protection_path = validator_dir.join(SLASHING_PROTECTION_FILENAME);
    let slashing_protection =
        SlashingDatabase::open_or_create(&slashing_protection_path).map_err(|e| {
            format!(
                "Unable to open or create slashing protection database at {}: {:?}",
                slashing_protection_path.display(),
                e
            )
        })?;

    // Create an empty transaction and drop it. Used to test if the database is locked.
    slashing_protection.test_transaction().map_err(|e| {
        format!(
            "Cannot import keys while the validator client is running: {:?}",
            e
        )
    })?;

    // Collect the paths for the keystores that should be imported.
    let keystore_paths = match (keystore, keystores_dir) {
        (Some(keystore), None) => vec![keystore],
        (None, Some(keystores_dir)) => {
            let mut keystores = vec![];

            recursively_find_voting_keystores(&keystores_dir, &mut keystores)
                .map_err(|e| format!("Unable to search {:?}: {:?}", keystores_dir, e))?;

            if keystores.is_empty() {
                eprintln!("No keystores found in {:?}", keystores_dir);
                return Ok(());
            }

            keystores
        }
        _ => {
            return Err(format!(
                "Must supply either --{} or --{}",
                KEYSTORE_FLAG, DIR_FLAG
            ))
        }
    };

    eprintln!("WARNING: {}", KEYSTORE_REUSE_WARNING);

    // For each keystore:
    //
    // - Obtain the keystore password, if the user desires.
    // - Copy the keystore into the `validator_dir`.
    // - Register the voting key with the slashing protection database.
    // - Add the keystore to the validator definitions file.
    //
    // Skip keystores that already exist, but exit early if any operation fails.
    // Reuses the same password for all keystores if the `REUSE_PASSWORD_FLAG` flag is set.
    let mut num_imported_keystores = 0;
    let mut previous_password: Option<ZeroizeString> = None;

    for src_keystore in &keystore_paths {
        let keystore = Keystore::from_json_file(src_keystore)
            .map_err(|e| format!("Unable to read keystore JSON {:?}: {:?}", src_keystore, e))?;

        eprintln!();
        eprintln!("Keystore found at {:?}:", src_keystore);
        eprintln!();
        eprintln!(" - Public key: 0x{}", keystore.pubkey());
        eprintln!(" - UUID: {}", keystore.uuid());
        eprintln!();
        eprintln!(
            "If you enter the password it will be stored as plain-text in {} so that it is not \
             required each time the validator client starts.",
            CONFIG_FILENAME
        );

        let password_opt = loop {
            if let Some(password) = previous_password.clone() {
                eprintln!("Reuse previous password.");
                break Some(password);
            }
            eprintln!();
            eprintln!("{}", PASSWORD_PROMPT);

            let password = match keystore_password_path.as_ref() {
                Some(path) => {
                    let password_from_file: ZeroizeString = fs::read_to_string(path)
                        .map_err(|e| format!("Unable to read {:?}: {:?}", path, e))?
                        .into();
                    password_from_file.without_newlines()
                }
                None => {
                    let password_from_user = read_password_from_user(stdin_inputs)?;
                    if password_from_user.as_ref().is_empty() {
                        eprintln!("Continuing without password.");
                        sleep(Duration::from_secs(1)); // Provides nicer UX.
                        break None;
                    }
                    password_from_user
                }
            };

            match keystore.decrypt_keypair(password.as_ref()) {
                Ok(_) => {
                    eprintln!("Password is correct.");
                    eprintln!();
                    sleep(Duration::from_secs(1)); // Provides nicer UX.
                    if reuse_password {
                        previous_password = Some(password.clone());
                    }
                    break Some(password);
                }
                Err(eth2_keystore::Error::InvalidPassword) => {
                    eprintln!("Invalid password");
                }
                Err(e) => return Err(format!("Error whilst decrypting keypair: {:?}", e)),
            }
        };

        let voting_pubkey = keystore
            .public_key()
            .ok_or_else(|| format!("Keystore public key is invalid: {}", keystore.pubkey()))?;

        // The keystore is placed in a directory that matches the name of the public key. This
        // provides some loose protection against adding the same keystore twice.
        let dest_dir = validator_dir.join(format!("0x{}", keystore.pubkey()));
        if dest_dir.exists() {
            // Check if we should update password for existing validator in case if it was provided via reimport: #2854
            let old_validator_def_opt = defs
                .as_mut_slice()
                .iter_mut()
                .find(|def| def.voting_public_key == voting_pubkey);
            if let Some(ValidatorDefinition {
                signing_definition:
                    SigningDefinition::LocalKeystore {
                        voting_keystore_password: ref mut old_passwd,
                        ..
                    },
                ..
            }) = old_validator_def_opt
            {
                if old_passwd.is_none() && password_opt.is_some() {
                    *old_passwd = password_opt;
                    defs.save(&validator_dir)
                        .map_err(|e| format!("Unable to save {}: {:?}", CONFIG_FILENAME, e))?;
                    eprintln!("Password updated for public key {}", voting_pubkey);
                }
            }
            eprintln!(
                "Skipping import of keystore for existing public key: {:?}",
                src_keystore
            );
            continue;
        }

        fs::create_dir_all(&dest_dir)
            .map_err(|e| format!("Unable to create import directory: {:?}", e))?;

        // Retain the keystore file name, but place it in the new directory.
        let dest_keystore = src_keystore
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .map(|file_name_str| dest_dir.join(file_name_str))
            .ok_or_else(|| format!("Badly formatted file name: {:?}", src_keystore))?;

        // Copy the keystore to the new location.
        fs::copy(src_keystore, &dest_keystore)
            .map_err(|e| format!("Unable to copy keystore: {:?}", e))?;

        // Register with slashing protection.
        slashing_protection
            .register_validator(voting_pubkey.compress())
            .map_err(|e| {
                format!(
                    "Error registering validator {}: {:?}",
                    voting_pubkey.as_hex_string(),
                    e
                )
            })?;

        eprintln!("Successfully imported keystore.");
        num_imported_keystores += 1;

        let graffiti = None;
        let suggested_fee_recipient = None;
        let validator_def = ValidatorDefinition::new_keystore_with_password(
            &dest_keystore,
            password_opt
                .map(PasswordStorage::ValidatorDefinitions)
                .unwrap_or(PasswordStorage::None),
            graffiti,
            suggested_fee_recipient,
            None,
            None,
            None,
            None,
        )
        .map_err(|e| format!("Unable to create new validator definition: {:?}", e))?;

        defs.push(validator_def);

        defs.save(&validator_dir)
            .map_err(|e| format!("Unable to save {}: {:?}", CONFIG_FILENAME, e))?;

        eprintln!("Successfully updated {}.", CONFIG_FILENAME);
    }

    eprintln!();
    eprintln!(
        "Successfully imported {} validators ({} skipped).",
        num_imported_keystores,
        keystore_paths.len() - num_imported_keystores
    );
    eprintln!();
    eprintln!("WARNING: {}", KEYSTORE_REUSE_WARNING);

    Ok(())
}
