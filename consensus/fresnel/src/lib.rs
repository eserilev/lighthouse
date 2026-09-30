//! Fresnel generates the Rust code of experimental consensus features from `registry.toml`.
//!
//! The generated file holds data only: feature identities, fork config keys and the
//! `for_each_feature!` macro. Feature logic stays hand-written behind feature gates.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt::{self, Write};
use std::io::Write as _;
use std::process::{Command, Stdio};

/// The real forks, in order. A feature must build on one of these, from `FIRST_FEATURE_FORK` on.
pub const FORKS: &[&str] = &[
    "Base",
    "Altair",
    "Bellatrix",
    "Capella",
    "Deneb",
    "Electra",
    "Fulu",
    "Gloas",
    "Heze",
];

pub const FIRST_FEATURE_FORK: &str = "Gloas";

/// The networks with a built-in `ChainSpec` constructor.
pub const NETWORKS: &[&str] = &["mainnet", "minimal", "gnosis"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registry {
    pub features: Vec<FeatureEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureEntry {
    pub name: String,
    pub min_fork: String,
    pub fork_versions: BTreeMap<String, [u8; 4]>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFeature {
    min_fork: String,
    fork_version: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryError {
    pub id: &'static str,
    pub message: String,
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.id, self.message)
    }
}

fn error(id: &'static str, message: impl Into<String>) -> RegistryError {
    RegistryError {
        id,
        message: message.into(),
    }
}

impl Registry {
    pub fn parse(source: &str) -> Result<Self, RegistryError> {
        let table: toml::Table =
            toml::from_str(source).map_err(|e| error("F-R00", format!("invalid TOML: {e}")))?;
        let mut features = Vec::with_capacity(table.len());
        for (name, value) in table {
            let raw: RawFeature = value
                .try_into()
                .map_err(|e| error("F-R05", format!("feature `{name}`: {e}")))?;
            let mut fork_versions = BTreeMap::new();
            for (network, version) in raw.fork_version {
                fork_versions.insert(network, parse_fork_version(&name, &version)?);
            }
            features.push(FeatureEntry {
                name,
                min_fork: raw.min_fork,
                fork_versions,
            });
        }
        let registry = Registry { features };
        registry.validate()?;
        Ok(registry)
    }

    fn validate(&self) -> Result<(), RegistryError> {
        let first_feature_fork = FORKS
            .iter()
            .position(|fork| *fork == FIRST_FEATURE_FORK)
            .unwrap_or(FORKS.len());
        let mut versions: BTreeMap<(&str, [u8; 4]), &str> = BTreeMap::new();

        for feature in &self.features {
            if !is_feature_name(&feature.name) {
                return Err(error(
                    "F-R01",
                    format!(
                        "feature name `{}` must be lower case ASCII letters, digits and `_`, \
                         and start with a letter",
                        feature.name
                    ),
                ));
            }
            match FORKS.iter().position(|fork| *fork == feature.min_fork) {
                Some(index) if index >= first_feature_fork => {}
                _ => {
                    return Err(error(
                        "F-R02",
                        format!(
                            "feature `{}`: min_fork `{}` is not a fork from {FIRST_FEATURE_FORK} on",
                            feature.name, feature.min_fork
                        ),
                    ));
                }
            }
            for network in NETWORKS {
                if !feature.fork_versions.contains_key(*network) {
                    return Err(error(
                        "F-R03",
                        format!(
                            "feature `{}`: fork_version has no value for `{network}`",
                            feature.name
                        ),
                    ));
                }
            }
            for (network, version) in &feature.fork_versions {
                if !NETWORKS.contains(&network.as_str()) {
                    return Err(error(
                        "F-R03",
                        format!(
                            "feature `{}`: fork_version has an unknown network `{network}`",
                            feature.name
                        ),
                    ));
                }
                if let Some(other) = versions.insert((network, *version), &feature.name) {
                    return Err(error(
                        "F-R04",
                        format!(
                            "features `{other}` and `{}` use the same {network} fork version",
                            feature.name
                        ),
                    ));
                }
            }
        }
        Ok(())
    }
}

fn is_feature_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn parse_fork_version(feature: &str, version: &str) -> Result<[u8; 4], RegistryError> {
    let invalid = || {
        error(
            "F-R03",
            format!(
                "feature `{feature}`: fork version `{version}` is not 4 bytes of 0x-prefixed hex"
            ),
        )
    };
    let hex = version.strip_prefix("0x").ok_or_else(invalid)?;
    if hex.len() != 8 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(invalid());
    }
    let mut bytes = [0u8; 4];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|_| invalid())?;
    }
    Ok(bytes)
}

/// `eip8198` becomes `Eip8198`, `eip_x` becomes `EipX`.
pub fn type_name(feature: &str) -> String {
    feature
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

fn version_literal(version: &[u8; 4]) -> String {
    let bytes: Vec<String> = version.iter().map(|b| format!("0x{b:02x}")).collect();
    format!("[{}]", bytes.join(", "))
}

/// Generate the unformatted source of `generated.rs`.
pub fn generate(registry: &Registry) -> Result<String, fmt::Error> {
    let features = &registry.features;
    let mut out = String::new();

    writeln!(
        out,
        "// @generated by fresnel from registry.toml. Do not edit. Run `make features`."
    )?;
    writeln!(out)?;
    if features.is_empty() {
        writeln!(out, "use crate::core::Epoch;")?;
        writeln!(out, "use crate::fork::ForkName;")?;
        writeln!(out, "use serde::{{Deserialize, Serialize}};")?;
    } else {
        writeln!(out, "use super::Feature;")?;
        writeln!(
            out,
            "use crate::core::{{Epoch, deserialize_fork_epoch, serialize_fork_epoch}};"
        )?;
        writeln!(out, "use crate::fork::ForkName;")?;
        writeln!(out, "use serde::{{Deserialize, Serialize}};")?;
        writeln!(out, "use serde_utils::quoted_u64::MaybeQuoted;")?;
    }
    writeln!(out)?;

    writeln!(
        out,
        "#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]"
    )?;
    writeln!(out, "pub enum FeatureId {{")?;
    for feature in features {
        writeln!(out, "{},", type_name(&feature.name))?;
    }
    writeln!(out, "}}")?;
    writeln!(out)?;

    writeln!(out, "impl FeatureId {{")?;
    writeln!(out, "/// All features, in registry order.")?;
    let all: Vec<String> = features
        .iter()
        .map(|f| format!("FeatureId::{}", type_name(&f.name)))
        .collect();
    writeln!(
        out,
        "pub const ALL: &'static [FeatureId] = &[{}];",
        all.join(", ")
    )?;
    writeln!(out)?;
    writeln!(out, "pub fn name(self) -> &'static str {{")?;
    writeln!(out, "match self {{")?;
    for feature in features {
        writeln!(
            out,
            "FeatureId::{} => \"{}\",",
            type_name(&feature.name),
            feature.name
        )?;
    }
    writeln!(out, "}}")?;
    writeln!(out, "}}")?;
    writeln!(out)?;
    writeln!(out, "/// The first fork that the feature can run on.")?;
    writeln!(out, "pub fn min_fork(self) -> ForkName {{")?;
    writeln!(out, "match self {{")?;
    for feature in features {
        writeln!(
            out,
            "FeatureId::{} => ForkName::{},",
            type_name(&feature.name),
            feature.min_fork
        )?;
    }
    writeln!(out, "}}")?;
    writeln!(out, "}}")?;
    writeln!(out, "}}")?;

    for feature in features {
        let ty = type_name(&feature.name);
        writeln!(out)?;
        writeln!(out, "pub struct {ty};")?;
        writeln!(out)?;
        writeln!(out, "impl Feature for {ty} {{")?;
        writeln!(out, "const ID: FeatureId = FeatureId::{ty};")?;
        writeln!(out, "const NAME: &'static str = \"{}\";", feature.name)?;
        writeln!(
            out,
            "const MIN_FORK: ForkName = ForkName::{};",
            feature.min_fork
        )?;
        writeln!(out, "}}")?;
    }

    writeln!(out)?;
    writeln!(out, "/// The fork version and fork epoch of each feature.")?;
    writeln!(
        out,
        "#[cfg_attr(feature = \"arbitrary\", derive(arbitrary::Arbitrary))]"
    )?;
    writeln!(out, "#[derive(Debug, Clone, PartialEq)]")?;
    writeln!(out, "pub struct FeatureSpec {{")?;
    for feature in features {
        writeln!(out, "pub {}_fork_version: [u8; 4],", feature.name)?;
        writeln!(out, "/// `None` means that the feature never activates.")?;
        writeln!(out, "pub {}_fork_epoch: Option<Epoch>,", feature.name)?;
    }
    writeln!(out, "}}")?;
    writeln!(out)?;

    writeln!(out, "impl FeatureSpec {{")?;
    for network in NETWORKS {
        writeln!(out, "pub fn {network}() -> Self {{")?;
        writeln!(out, "Self {{")?;
        for feature in features {
            let version = feature
                .fork_versions
                .get(*network)
                .map(version_literal)
                .unwrap_or_default();
            writeln!(out, "{}_fork_version: {version},", feature.name)?;
            writeln!(out, "{}_fork_epoch: None,", feature.name)?;
        }
        writeln!(out, "}}")?;
        writeln!(out, "}}")?;
        writeln!(out)?;
    }

    writeln!(
        out,
        "pub fn fork_version(&self, id: FeatureId) -> [u8; 4] {{"
    )?;
    writeln!(out, "match id {{")?;
    for feature in features {
        writeln!(
            out,
            "FeatureId::{} => self.{}_fork_version,",
            type_name(&feature.name),
            feature.name
        )?;
    }
    writeln!(out, "}}")?;
    writeln!(out, "}}")?;
    writeln!(out)?;

    writeln!(
        out,
        "pub fn fork_epoch(&self, id: FeatureId) -> Option<Epoch> {{"
    )?;
    writeln!(out, "match id {{")?;
    for feature in features {
        writeln!(
            out,
            "FeatureId::{} => self.{}_fork_epoch,",
            type_name(&feature.name),
            feature.name
        )?;
    }
    writeln!(out, "}}")?;
    writeln!(out, "}}")?;
    writeln!(out, "}}")?;
    writeln!(out)?;

    writeln!(
        out,
        "/// The config keys of each feature. `Config` flattens this struct."
    )?;
    writeln!(
        out,
        "#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]"
    )?;
    writeln!(out, "pub struct FeatureConfig {{")?;
    for feature in features {
        let key = feature.name.to_ascii_uppercase();
        writeln!(
            out,
            "#[serde(rename = \"{key}_FORK_VERSION\", default = \"default_fork_version\", with = \"serde_utils::bytes_4_hex\")]"
        )?;
        writeln!(out, "pub {}_fork_version: [u8; 4],", feature.name)?;
        writeln!(
            out,
            "#[serde(rename = \"{key}_FORK_EPOCH\", default, serialize_with = \"serialize_fork_epoch\", deserialize_with = \"deserialize_fork_epoch\")]"
        )?;
        writeln!(
            out,
            "pub {}_fork_epoch: Option<MaybeQuoted<Epoch>>,",
            feature.name
        )?;
    }
    writeln!(out, "}}")?;
    writeln!(out)?;

    writeln!(out, "impl FeatureConfig {{")?;
    let spec_arg = if features.is_empty() { "_spec" } else { "spec" };
    writeln!(out, "pub fn from_spec({spec_arg}: &FeatureSpec) -> Self {{")?;
    writeln!(out, "Self {{")?;
    for feature in features {
        writeln!(
            out,
            "{name}_fork_version: spec.{name}_fork_version,",
            name = feature.name
        )?;
        writeln!(
            out,
            "{name}_fork_epoch: spec.{name}_fork_epoch.map(|value| MaybeQuoted {{ value }}),",
            name = feature.name
        )?;
    }
    writeln!(out, "}}")?;
    writeln!(out, "}}")?;
    writeln!(out)?;
    writeln!(out, "pub fn to_spec(&self) -> FeatureSpec {{")?;
    writeln!(out, "FeatureSpec {{")?;
    for feature in features {
        writeln!(
            out,
            "{name}_fork_version: self.{name}_fork_version,",
            name = feature.name
        )?;
        writeln!(
            out,
            "{name}_fork_epoch: self.{name}_fork_epoch.map(|epoch| epoch.value),",
            name = feature.name
        )?;
    }
    writeln!(out, "}}")?;
    writeln!(out, "}}")?;
    writeln!(out, "}}")?;
    writeln!(out)?;

    if !features.is_empty() {
        writeln!(out, "fn default_fork_version() -> [u8; 4] {{")?;
        writeln!(out, "[0xff, 0xff, 0xff, 0xff]")?;
        writeln!(out, "}}")?;
        writeln!(out)?;
    }

    writeln!(
        out,
        "/// Calls `$m!(FeatureType)` once for each feature, in registry order."
    )?;
    writeln!(out, "#[macro_export]")?;
    writeln!(out, "macro_rules! for_each_feature {{")?;
    writeln!(out, "($m:ident) => {{")?;
    for feature in features {
        writeln!(out, "$m!($crate::features::{});", type_name(&feature.name))?;
    }
    writeln!(out, "}};")?;
    writeln!(out, "}}")?;

    Ok(out)
}

/// Format Rust source with `rustfmt`, so that the output is stable and passes `cargo fmt`.
pub fn rustfmt(source: &str) -> Result<String, String> {
    let mut child = Command::new("rustfmt")
        .args(["--edition", "2024", "--emit", "stdout"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("cannot run rustfmt: {e}"))?;
    child
        .stdin
        .take()
        .ok_or("cannot open the rustfmt stdin")?
        .write_all(source.as_bytes())
        .map_err(|e| format!("cannot write to rustfmt: {e}"))?;
    let output = child
        .wait_with_output()
        .map_err(|e| format!("rustfmt failed: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "rustfmt failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    String::from_utf8(output.stdout).map_err(|e| format!("rustfmt output is not UTF-8: {e}"))
}
