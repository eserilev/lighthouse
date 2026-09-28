mod json;
mod justifiability;
mod ssz_static;
mod state_transition;

use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

pub use justifiability::JustifiabilityCase;
pub use ssz_static::SszCase;
pub use state_transition::StateTransitionCase;

pub const FORK: &str = "lstar";

pub enum Outcome {
    Passed,
    Skipped(&'static str),
}

pub trait Case: DeserializeOwned {
    const CATEGORY: &'static str;

    fn run(&self) -> Result<Outcome, String>;
}

#[derive(Debug, Default)]
pub struct Summary {
    pub passed: usize,
    pub skipped: BTreeMap<String, usize>,
    pub failures: Vec<String>,
}

pub fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("leanspec-fixtures/fixtures/consensus")
}

pub fn run_category<C: Case>() -> Summary {
    let dir = fixtures_root().join(C::CATEGORY).join(FORK);
    let mut files = vec![];
    if let Err(e) = collect_json_files(&dir, &mut files) {
        return Summary {
            failures: vec![format!("{}: {e}", dir.display())],
            ..Summary::default()
        };
    }

    let mut summary = Summary::default();
    if files.is_empty() {
        summary
            .failures
            .push(format!("{}: no fixtures found", dir.display()));
    }

    for file in files {
        let cases = match load_cases::<C>(&file) {
            Ok(cases) => cases,
            Err(e) => {
                summary.failures.push(format!("{}: {e}", file.display()));
                continue;
            }
        };
        for (name, case) in cases {
            match case.run() {
                Ok(Outcome::Passed) => summary.passed += 1,
                Ok(Outcome::Skipped(reason)) => {
                    *summary.skipped.entry(reason.to_string()).or_default() += 1
                }
                Err(e) => summary.failures.push(format!("{name}: {e}")),
            }
        }
    }
    summary
}

fn load_cases<C: Case>(file: &Path) -> Result<BTreeMap<String, C>, String> {
    let contents = fs::read_to_string(file).map_err(|e| e.to_string())?;
    serde_json::from_str(&contents).map_err(|e| e.to_string())
}

fn collect_json_files(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = fs::read_dir(dir).map_err(|e| e.to_string())?;
    for entry in entries {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.is_dir() {
            collect_json_files(&path, files)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            files.push(path);
        }
    }
    files.sort();
    Ok(())
}

pub fn decode_hex(value: &str) -> Result<Vec<u8>, String> {
    hex::decode(value.strip_prefix("0x").unwrap_or(value)).map_err(|e| e.to_string())
}
