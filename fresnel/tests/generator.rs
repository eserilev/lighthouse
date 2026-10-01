use fresnel::{Registry, generate, read_forks, rustfmt, type_name};

const TWO_FEATURES: &str = include_str!("fixtures/two_features.toml");
const TEST_FEATURES: &str = include_str!("fixtures/test_features.toml");
const FORK_NAME_SOURCE: &str = include_str!("../../consensus/types/src/fork/fork_name.rs");

fn forks() -> Vec<String> {
    read_forks(FORK_NAME_SOURCE).expect("fork names")
}

fn generated(source: &str) -> String {
    let registry = Registry::parse(source, &forks()).expect("valid registry");
    rustfmt(&generate(&registry).expect("generate")).expect("rustfmt")
}

fn parse_error(source: &str) -> &'static str {
    Registry::parse(source, &forks())
        .expect_err("invalid registry")
        .id
}

#[test]
fn fork_names_come_from_the_fork_name_enum() {
    let forks = forks();
    assert_eq!(forks.first().map(String::as_str), Some("Base"));
    for fork in ["Altair", "Fulu", "Gloas", "Heze"] {
        assert!(
            forks.iter().any(|f| f == fork),
            "{fork} missing in {forks:?}"
        );
    }
    assert_eq!(
        read_forks("pub struct Nope {}").expect_err("no enum").id,
        "F-R06"
    );
}

#[test]
fn registry_order_is_kept() {
    let registry = Registry::parse(TWO_FEATURES, &forks()).expect("valid registry");
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
        "pub eip8198_fork_version: Option<[u8; 4]>,",
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
fn checked_in_files_match_the_registry() {
    let root = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../consensus/types/src/features"
    );
    let read = |file: &str| std::fs::read_to_string(format!("{root}/{file}")).expect(file);
    let registry = Registry::parse(&read("registry.toml"), &forks()).expect("valid registry");
    let fixture = Registry::parse(TEST_FEATURES, &forks()).expect("valid fixture");
    let with_fixture = registry
        .with_fixture(&fixture, &forks())
        .expect("valid registry and fixture");
    let format = |registry: &Registry| rustfmt(&generate(registry).expect("generate")).unwrap();

    assert_eq!(
        format(&registry),
        read("generated.rs"),
        "run `make features`"
    );
    assert_eq!(
        format(&with_fixture),
        read("generated_fixture.rs"),
        "run `make features`"
    );
}

#[test]
fn fixture_features_follow_the_registry_features() {
    let registry = Registry::parse(TWO_FEATURES, &forks()).expect("valid registry");
    let fixture = Registry::parse(TEST_FEATURES, &forks()).expect("valid fixture");
    let with_fixture = registry
        .with_fixture(&fixture, &forks())
        .expect("valid registry and fixture");
    let names: Vec<&str> = with_fixture
        .features
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(
        names,
        [
            "eip8198",
            "eip_toy",
            "heze_test_feature",
            "gloas_test_feature"
        ]
    );

    assert_eq!(
        registry
            .with_fixture(&registry, &forks())
            .expect_err("duplicate features")
            .id,
        "F-R07"
    );
    let versions = r#"fork_version = { mainnet = "0x01000000", minimal = "0x01000001", gnosis = "0x01000064" }"#;
    let clash = Registry::parse(
        &format!("[eip_toy_2]\nmin_fork = \"Heze\"\n{versions}"),
        &forks(),
    )
    .expect("valid fixture");
    let clash_with = Registry::parse(
        &format!("[eip1]\nmin_fork = \"Heze\"\n{versions}"),
        &forks(),
    )
    .expect("valid registry");
    assert_eq!(
        clash_with
            .with_fixture(&clash, &forks())
            .expect_err("same fork version")
            .id,
        "F-R04"
    );
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
    assert_eq!(
        parse_error(&format!(
            "[eip_1]\nmin_fork = \"Heze\"\n{versions}\n[eip1]\nmin_fork = \"Heze\"\nfork_version = {{ mainnet = \"0x02000000\", minimal = \"0x02000001\", gnosis = \"0x02000064\" }}"
        )),
        "F-R07"
    );
}
