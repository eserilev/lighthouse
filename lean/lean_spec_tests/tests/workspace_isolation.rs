use cargo_metadata::{MetadataCommand, PackageId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;

const ETH2_CRATES: &[&str] = &[
    "beacon_chain",
    "fork_choice",
    "lighthouse_network",
    "proto_array",
    "state_processing",
    "store",
    "types",
];

#[test]
fn lean_crates_do_not_depend_on_eth2_crates() {
    let metadata = MetadataCommand::new()
        .manifest_path(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.toml"))
        .exec()
        .expect("cargo metadata");
    let resolve = metadata
        .resolve
        .as_ref()
        .expect("resolved dependency graph");

    let names: HashMap<&PackageId, &str> = metadata
        .packages
        .iter()
        .map(|package| (&package.id, package.name.as_str()))
        .collect();
    let dependencies: HashMap<&PackageId, Vec<&PackageId>> = resolve
        .nodes
        .iter()
        .map(|node| (&node.id, node.dependencies.iter().collect()))
        .collect();
    let workspace_members: HashSet<&PackageId> = metadata.workspace_members.iter().collect();
    let is_eth2_crate =
        |id: &PackageId| workspace_members.contains(id) && ETH2_CRATES.contains(&names[id]);

    let mut violations = vec![];
    for lean_crate in workspace_members
        .iter()
        .filter(|id| names[*id].starts_with("lean_"))
    {
        let mut parents: HashMap<&PackageId, &PackageId> = HashMap::new();
        let mut queue = VecDeque::from([*lean_crate]);
        while let Some(id) = queue.pop_front() {
            for dependency in &dependencies[id] {
                if *dependency == *lean_crate || parents.contains_key(dependency) {
                    continue;
                }
                parents.insert(dependency, id);
                if is_eth2_crate(dependency) {
                    let mut path = vec![names[*dependency]];
                    let mut current = *dependency;
                    while let Some(parent) = parents.get(current) {
                        path.push(names[*parent]);
                        current = parent;
                    }
                    path.reverse();
                    violations.push(path.join(" -> "));
                } else {
                    queue.push_back(dependency);
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "lean crates must not depend on eth2 crates:\n{}",
        violations.join("\n")
    );
}
