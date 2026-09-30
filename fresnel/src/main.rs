use clap::Parser;
use fresnel::delete::{remove_dispatch_attributes, remove_mod_declarations, remove_registry_table};
use fresnel::footprint::{footprint, in_feature_dir, manifest, uses};
use fresnel::{Registry, generate, read_forks, rustfmt, type_name};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

/// Generate the experimental feature code from the feature registry.
///
/// Run from the workspace root.
#[derive(Parser)]
struct Cli {
    /// Path to `registry.toml`.
    #[arg(long, default_value = "consensus/types/src/features/registry.toml")]
    registry: PathBuf,
    /// Path to the test features that the tests of `types` add to the registry.
    #[arg(long, default_value = "fresnel/tests/fixtures/test_features.toml")]
    fixture: PathBuf,
    /// Path to the source file of the `ForkName` enum.
    #[arg(long, default_value = "consensus/types/src/fork/fork_name.rs")]
    forks: PathBuf,
    /// Path to the generated Rust file.
    #[arg(long, default_value = "consensus/types/src/features/generated.rs")]
    out: PathBuf,
    /// Path to the generated Rust file of the registry and the test features.
    #[arg(
        long,
        default_value = "consensus/types/src/features/generated_fixture.rs"
    )]
    fixture_out: PathBuf,
    /// Compare the generated files with the files on disk and write nothing.
    #[arg(long)]
    check: bool,
    /// Delete a feature: its registry table, its dispatch attributes, its directories and their
    /// module declarations. Then generate the code and print the remaining uses of the feature.
    #[arg(long, value_name = "FEATURE", conflicts_with = "check")]
    delete: Option<String>,
}

/// An error message and the exit code: 4 if `--check` finds a difference, 1 otherwise.
type Failure = (String, u8);

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match &cli.delete {
        Some(feature) => delete_feature(&cli, feature),
        None => run(&cli),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err((message, code)) => {
            eprintln!("{message}");
            ExitCode::from(code)
        }
    }
}

fn read(path: &Path) -> Result<String, Failure> {
    std::fs::read_to_string(path).map_err(|e| (format!("cannot read {}: {e}", path.display()), 1))
}

fn write(path: &Path, contents: &str) -> Result<(), Failure> {
    std::fs::write(path, contents).map_err(|e| (format!("cannot write {}: {e}", path.display()), 1))
}

/// Write `contents` to `path`, or with `check`, compare them with the file.
fn write_or_check(path: &Path, contents: &str, check: bool) -> Result<(), Failure> {
    if !check {
        return write(path, contents);
    }
    if std::fs::read_to_string(path).unwrap_or_default() != contents {
        return Err((
            format!("{} is out of date. Run `make features`.", path.display()),
            4,
        ));
    }
    Ok(())
}

fn run(cli: &Cli) -> Result<(), Failure> {
    let forks = read_forks(&read(&cli.forks)?).map_err(|e| (e.to_string(), 1))?;
    let registry = Registry::parse(&read(&cli.registry)?, &forks)
        .map_err(|e| (format!("{}: {e}", cli.registry.display()), 1))?;
    let with_fixture = Registry::parse(&read(&cli.fixture)?, &forks)
        .and_then(|fixture| registry.with_fixture(&fixture, &forks))
        .map_err(|e| (format!("{}: {e}", cli.fixture.display()), 1))?;

    for (registry, out) in [(&registry, &cli.out), (&with_fixture, &cli.fixture_out)] {
        let generated = generate(registry)
            .map_err(|e| e.to_string())
            .and_then(|code| rustfmt(&code))
            .map_err(|e| (e, 1))?;
        write_or_check(out, &generated, cli.check)?;
    }

    let files = rust_files(cli)?;
    let features_dir = cli.out.parent().unwrap_or(Path::new(""));
    let mut violations = vec![];
    for feature in &registry.features {
        let name = type_name(&feature.name);
        let footprint = footprint(&feature.name, &name, &files);
        let dir = features_dir.join(&feature.name);
        if dir.is_dir() {
            let contents = manifest(&feature.name, &name, &footprint.touches);
            write_or_check(&dir.join("FOOTPRINT.md"), &contents, cli.check)?;
        }
        violations.extend(footprint.violations.iter().map(|line| {
            format!(
                "{line}: `{name}` outside `features/{}/` must be `feature_enabled::<{name}>`, \
                 `feature_fork_epoch(FeatureId::{name})`, `#[feature_dispatch({name} => ...)]` \
                 or an import",
                feature.name
            )
        }));
    }
    if !violations.is_empty() {
        return Err((violations.join("\n"), 1));
    }
    Ok(())
}

fn delete_feature(cli: &Cli, feature: &str) -> Result<(), Failure> {
    let forks = read_forks(&read(&cli.forks)?).map_err(|e| (e.to_string(), 1))?;
    let source = read(&cli.registry)?;
    let registry = Registry::parse(&source, &forks)
        .map_err(|e| (format!("{}: {e}", cli.registry.display()), 1))?;
    if !registry.features.iter().any(|entry| entry.name == feature) {
        return Err((
            format!("{} has no feature `{feature}`", cli.registry.display()),
            1,
        ));
    }
    write(&cli.registry, &remove_registry_table(&source, feature))?;

    let name = type_name(feature);
    let files = rust_files(cli)?;
    let dir_path = format!("features/{feature}");
    let feature_dirs: BTreeSet<&str> = files
        .iter()
        .filter(|(path, _)| in_feature_dir(path, feature))
        .filter_map(|(path, _)| {
            let start = path.find(&format!("{dir_path}/"))?;
            path.get(..start + dir_path.len())
        })
        .collect();
    let parent_modules: Vec<String> = feature_dirs
        .iter()
        .flat_map(|dir| {
            let parent = dir.trim_end_matches(&format!("/{feature}"));
            [format!("{parent}/mod.rs"), format!("{parent}.rs")]
        })
        .collect();

    for (path, source) in &files {
        if in_feature_dir(path, feature) {
            continue;
        }
        let mut edited = remove_dispatch_attributes(source, &name);
        if parent_modules.contains(path) {
            edited = remove_mod_declarations(&edited, feature);
        }
        if edited != *source {
            write(Path::new(path), &edited)?;
        }
    }

    for dir in &feature_dirs {
        let status = Command::new("git")
            .args(["rm", "-r", "-q", dir])
            .status()
            .map_err(|e| (format!("cannot run git: {e}"), 1))?;
        if !status.success() {
            return Err((format!("git rm -r {dir} failed"), 1));
        }
    }

    run(cli)?;

    let remaining = uses(&name, &rust_files(cli)?);
    if remaining.is_empty() {
        println!("Deleted `{feature}`. No uses of `{name}` remain.");
    } else {
        println!("Deleted `{feature}`. Remove the remaining uses of `{name}`:");
        for line in remaining {
            println!("{line}");
        }
    }
    Ok(())
}

/// The workspace Rust files, as `(path, source)` pairs sorted by path. Skips `target` and hidden
/// directories, the generated files and this crate.
fn rust_files(cli: &Cli) -> Result<Vec<(String, String)>, Failure> {
    let this_crate = std::env::current_dir().ok().and_then(|dir| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .strip_prefix(dir)
            .ok()
            .map(Path::to_path_buf)
    });
    let skip = |path: &Path| {
        path == cli.out || path == cli.fixture_out || Some(path) == this_crate.as_deref()
    };

    let mut files = vec![];
    let mut dirs = vec![PathBuf::new()];
    while let Some(dir) = dirs.pop() {
        let entries = std::fs::read_dir(Path::new(".").join(&dir))
            .map_err(|e| (format!("cannot read {}: {e}", dir.display()), 1))?;
        for entry in entries {
            let entry = entry.map_err(|e| (format!("cannot read {}: {e}", dir.display()), 1))?;
            let file_name = entry.file_name().to_string_lossy().into_owned();
            let path = dir.join(&file_name);
            if file_name.starts_with('.') || file_name == "target" || skip(&path) {
                continue;
            }
            if entry.file_type().is_ok_and(|file_type| file_type.is_dir()) {
                dirs.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push((path.to_string_lossy().into_owned(), read(&path)?));
            }
        }
    }
    files.sort();
    Ok(files)
}
