use clap::Parser;
use fresnel::{Registry, generate, read_forks, rustfmt};
use std::path::PathBuf;
use std::process::ExitCode;

/// Generate the experimental feature code from the feature registry.
#[derive(Parser)]
struct Cli {
    /// Path to `registry.toml`.
    #[arg(long, default_value = "consensus/types/src/features/registry.toml")]
    registry: PathBuf,
    /// Path to the source file of the `ForkName` enum.
    #[arg(long, default_value = "consensus/types/src/fork/fork_name.rs")]
    forks: PathBuf,
    /// Path to the generated Rust file.
    #[arg(long, default_value = "consensus/types/src/features/generated.rs")]
    out: PathBuf,
    /// Compare the generated code with `--out` and write nothing.
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

/// Returns the error message and the exit code: 4 if `--check` finds a difference, 1 otherwise.
fn run(cli: &Cli) -> Result<(), (String, u8)> {
    let source = std::fs::read_to_string(&cli.registry)
        .map_err(|e| (format!("cannot read {}: {e}", cli.registry.display()), 1))?;
    let fork_source = std::fs::read_to_string(&cli.forks)
        .map_err(|e| (format!("cannot read {}: {e}", cli.forks.display()), 1))?;
    let forks = read_forks(&fork_source).map_err(|e| (e.to_string(), 1))?;
    let registry = Registry::parse(&source, &forks)
        .map_err(|e| (format!("{}: {e}", cli.registry.display()), 1))?;
    let generated = generate(&registry)
        .map_err(|e| e.to_string())
        .and_then(|code| rustfmt(&code))
        .map_err(|e| (e, 1))?;

    if cli.check {
        let current = std::fs::read_to_string(&cli.out).unwrap_or_default();
        if current != generated {
            return Err((
                format!("{} is out of date. Run `make features`.", cli.out.display()),
                4,
            ));
        }
        return Ok(());
    }

    std::fs::write(&cli.out, generated)
        .map_err(|e| (format!("cannot write {}: {e}", cli.out.display()), 1))
}
