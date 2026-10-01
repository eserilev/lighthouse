use clap::Parser;
use fresnel::{Registry, generate, read_forks, rustfmt};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// Generate the experimental feature code from the feature registry.
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
    /// Compare the generated code with `--out` and `--fixture-out` and write nothing.
    #[arg(long)]
    check: bool,
}

fn main() -> ExitCode {
    match run(&Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err((message, code)) => {
            eprintln!("{message}");
            ExitCode::from(code)
        }
    }
}

fn read(path: &Path) -> Result<String, (String, u8)> {
    std::fs::read_to_string(path).map_err(|e| (format!("cannot read {}: {e}", path.display()), 1))
}

/// Returns the error message and the exit code: 4 if `--check` finds a difference, 1 otherwise.
fn run(cli: &Cli) -> Result<(), (String, u8)> {
    let forks = read_forks(&read(&cli.forks)?).map_err(|e| (e.to_string(), 1))?;
    let registry = Registry::parse(&read(&cli.registry)?, &forks)
        .map_err(|e| (format!("{}: {e}", cli.registry.display()), 1))?;
    let with_fixture = Registry::parse(&read(&cli.fixture)?, &forks)
        .and_then(|fixture| registry.with_fixture(&fixture, &forks))
        .map_err(|e| (format!("{}: {e}", cli.fixture.display()), 1))?;

    for (registry, out) in [(registry, &cli.out), (with_fixture, &cli.fixture_out)] {
        let generated = generate(&registry)
            .map_err(|e| e.to_string())
            .and_then(|code| rustfmt(&code))
            .map_err(|e| (e, 1))?;

        if cli.check {
            let current = std::fs::read_to_string(out).unwrap_or_default();
            if current != generated {
                return Err((
                    format!("{} is out of date. Run `make features`.", out.display()),
                    4,
                ));
            }
        } else {
            std::fs::write(out, generated)
                .map_err(|e| (format!("cannot write {}: {e}", out.display()), 1))?;
        }
    }
    Ok(())
}
