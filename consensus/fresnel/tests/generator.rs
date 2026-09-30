use fresnel::{Registry, generate, rustfmt, type_name};

const TWO_FEATURES: &str = include_str!("fixtures/two_features.toml");

fn generated(source: &str) -> String {
    let registry = Registry::parse(source).expect("valid registry");
    rustfmt(&generate(&registry).expect("generate")).expect("rustfmt")
}

fn parse_error(source: &str) -> &'static str {
    Registry::parse(source).expect_err("invalid registry").id
}

#[test]
fn registry_order_is_kept() {
    let registry = Registry::parse(TWO_FEATURES).expect("valid registry");
    let names: Vec<&str> = registry.features.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["eip8198", "eip_toy"]);
}

#[test]
fn output_is_deterministic() {
    assert_eq!(generated(TWO_FEATURES), generated(TWO_FEATURES));
}

#[test]
fn output_lists_each_feature() {
    let code = generated(TWO_FEATURES);
    for expected in [
        "pub enum FeatureId {\n    Eip8198,\n    EipToy,\n}",
        "pub const ALL: &'static [FeatureId] = &[FeatureId::Eip8198, FeatureId::EipToy];",
        "const MIN_FORK: ForkName = ForkName::Heze;",
        "const MIN_FORK: ForkName = ForkName::Gloas;",
        "eip8198_fork_version: [0xe8, 0x19, 0x80, 0x00],",
        "eip8198_fork_version: [0xe8, 0x19, 0x80, 0x01],",
        "eip8198_fork_version: [0xe8, 0x19, 0x80, 0x64],",
        "rename = \"EIP8198_FORK_VERSION\"",
        "rename = \"EIP_TOY_FORK_EPOCH\"",
        "$m!($crate::features::Eip8198);\n        $m!($crate::features::EipToy);",
    ] {
        assert!(code.contains(expected), "missing `{expected}` in:\n{code}");
    }
}

#[test]
fn empty_registry_generates_no_features() {
    let code = generated("");
    assert!(code.contains("pub enum FeatureId {}"), "{code}");
    assert!(
        code.contains("pub const ALL: &'static [FeatureId] = &[];"),
        "{code}"
    );
}

#[test]
fn checked_in_file_matches_the_registry() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../types/src/features");
    let registry = std::fs::read_to_string(format!("{root}/registry.toml")).expect("registry");
    let checked_in = std::fs::read_to_string(format!("{root}/generated.rs")).expect("generated");
    assert_eq!(generated(&registry), checked_in, "run `make features`");
}

#[test]
fn type_names() {
    assert_eq!(type_name("eip8198"), "Eip8198");
    assert_eq!(type_name("eip_toy"), "EipToy");
}

#[test]
fn invalid_registries_are_rejected() {
    let versions = r#"fork_version = { mainnet = "0x01000000", minimal = "0x01000001", gnosis = "0x01000064" }"#;
    assert_eq!(parse_error("[eip = 1"), "F-R00");
    assert_eq!(
        parse_error(&format!("[Eip1]\nmin_fork = \"Heze\"\n{versions}")),
        "F-R01"
    );
    assert_eq!(
        parse_error(&format!("[eip1]\nmin_fork = \"Fulu\"\n{versions}")),
        "F-R02"
    );
    assert_eq!(
        parse_error(&format!("[eip1]\nmin_fork = \"Nope\"\n{versions}")),
        "F-R02"
    );
    assert_eq!(
        parse_error(
            "[eip1]\nmin_fork = \"Heze\"\nfork_version = { mainnet = \"0x01000000\", minimal = \"0x01000001\" }"
        ),
        "F-R03"
    );
    assert_eq!(
        parse_error(
            "[eip1]\nmin_fork = \"Heze\"\nfork_version = { mainnet = \"0x010000\", minimal = \"0x01000001\", gnosis = \"0x01000064\" }"
        ),
        "F-R03"
    );
    assert_eq!(
        parse_error(&format!(
            "[eip1]\nmin_fork = \"Heze\"\n{versions}\n[eip2]\nmin_fork = \"Heze\"\n{versions}"
        )),
        "F-R04"
    );
    assert_eq!(
        parse_error(&format!(
            "[eip1]\nmin_fork = \"Heze\"\nnope = 1\n{versions}"
        )),
        "F-R05"
    );
}
