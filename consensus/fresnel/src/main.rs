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
    let cli = Cli::parse();

    let source = match std::fs::read_to_string(&cli.registry) {
        Ok(source) => source,
        Err(e) => {
            eprintln!("cannot read {}: {e}", cli.registry.display());
            return ExitCode::from(1);
        }
    };
    let forks = match std::fs::read_to_string(&cli.forks)
        .map_err(|e| format!("cannot read {}: {e}", cli.forks.display()))
        .and_then(|fork_source| read_forks(&fork_source).map_err(|e| e.to_string()))
    {
        Ok(forks) => forks,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(1);
        }
    };
    let registry = match Registry::parse(&source, &forks) {
        Ok(registry) => registry,
        Err(e) => {
            eprintln!("{}: {e}", cli.registry.display());
            return ExitCode::from(1);
        }
    };
    let generated = match generate(&registry)
        .map_err(|e| e.to_string())
        .and_then(|code| rustfmt(&code))
    {
        Ok(generated) => generated,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(1);
        }
    };

    if cli.check {
        let current = std::fs::read_to_string(&cli.out).unwrap_or_default();
        if current != generated {
            eprintln!("{} is out of date. Run `make features`.", cli.out.display());
            return ExitCode::from(4);
        }
        return ExitCode::SUCCESS;
    }

    if let Err(e) = std::fs::write(&cli.out, generated) {
        eprintln!("cannot write {}: {e}", cli.out.display());
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}
