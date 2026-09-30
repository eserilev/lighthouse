//! The footprint of a feature: the lines outside its own directories that name it.
//!
//! Outside `*/features/<name>/`, a feature type `Name` may only appear in:
//!
//! - `feature_enabled::<Name>`
//! - `feature_fork_epoch(FeatureId::Name)`
//! - `#[feature_dispatch(Name => ...)]`
//! - `use` statements
//!
//! The scan is line based: a `use` statement ends at the first line with `;`, and a
//! `#[feature_dispatch(` attribute ends at the first line with `)]`.

use std::collections::BTreeSet;

/// A line of a Rust source file, with a 1-based line number and the trimmed code.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Line {
    pub path: String,
    pub number: usize,
    pub text: String,
}

impl std::fmt::Display for Line {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.path, self.number)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Footprint {
    /// Lines that name the feature in an allowed form.
    pub touches: Vec<Line>,
    /// Lines that name the feature in any other form.
    pub violations: Vec<Line>,
}

/// Returns `true` if `path` is in a directory of `feature`, such as `src/features/eip1234/`.
pub fn in_feature_dir(path: &str, feature: &str) -> bool {
    let dir = format!("features/{feature}/");
    path.starts_with(&dir) || path.contains(&format!("/{dir}"))
}

/// The text after each whole-identifier `word` in `line`.
fn after_word<'a>(line: &'a str, word: &'a str) -> impl Iterator<Item = &'a str> {
    let is_ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
    line.match_indices(word).filter_map(move |(start, _)| {
        let after = &line[start + word.len()..];
        (!line[..start].ends_with(is_ident) && !after.starts_with(is_ident)).then_some(after)
    })
}

/// `text` as a Markdown code span, with a fence longer than each backtick run in `text`.
fn code_span(text: &str) -> String {
    let longest_run = text.split(|c| c != '`').map(str::len).max().unwrap_or(0);
    let fence = "`".repeat(longest_run + 1);
    let pad = if longest_run > 0 { " " } else { "" };
    format!("{fence}{pad}{text}{pad}{fence}")
}

fn count_word(line: &str, word: &str) -> usize {
    after_word(line, word).count()
}

/// Scan `files`, as `(path, source)` pairs, for the type name `name` of `feature`.
pub fn footprint(feature: &str, name: &str, files: &[(String, String)]) -> Footprint {
    let mut footprint = Footprint::default();
    let allowed_forms = [
        format!("feature_enabled::<{name}>"),
        format!("feature_fork_epoch(FeatureId::{name})"),
    ];

    for (path, source) in files {
        if in_feature_dir(path, feature) {
            continue;
        }
        let mut in_use = false;
        let mut in_dispatch = false;
        for (index, line) in source.lines().enumerate() {
            let trimmed = line.trim_start();
            in_use |= ["use ", "pub use ", "pub(crate) use ", "pub(super) use "]
                .iter()
                .any(|prefix| trimmed.starts_with(prefix));
            in_dispatch |= trimmed.starts_with("#[feature_dispatch(");

            let uses = count_word(line, name);
            let allowed = if in_use {
                uses
            } else {
                let mut allowed = allowed_forms
                    .iter()
                    .map(|form| line.matches(form.as_str()).count())
                    .sum();
                if in_dispatch {
                    allowed += after_word(line, name)
                        .filter(|after| after.trim_start().starts_with("=>"))
                        .count();
                }
                allowed
            };

            if uses > 0 {
                let line = Line {
                    path: path.clone(),
                    number: index + 1,
                    text: line.trim().to_string(),
                };
                if uses > allowed {
                    footprint.violations.push(line);
                } else {
                    footprint.touches.push(line);
                }
            }

            in_use &= !line.contains(';');
            in_dispatch &= !line.contains(")]");
        }
    }
    footprint
}

/// Every line of `files` that names `name`.
pub fn uses(name: &str, files: &[(String, String)]) -> Vec<Line> {
    files
        .iter()
        .flat_map(|(path, source)| {
            source
                .lines()
                .enumerate()
                .filter(|(_, line)| count_word(line, name) > 0)
                .map(|(index, line)| Line {
                    path: path.clone(),
                    number: index + 1,
                    text: line.trim().to_string(),
                })
        })
        .collect()
}

/// The contents of `FOOTPRINT.md` for `feature`: each touch as its path and code, sorted and
/// deduplicated. Line numbers are left out, so that an unrelated edit does not change the file.
pub fn manifest(feature: &str, name: &str, touches: &[Line]) -> String {
    let touches: BTreeSet<(&str, &str)> = touches
        .iter()
        .map(|touch| (touch.path.as_str(), touch.text.as_str()))
        .collect();
    let list: String = touches
        .into_iter()
        .map(|(path, text)| format!("\n- {}: {}", code_span(path), code_span(text)))
        .collect();
    format!(
        "<!-- @generated by fresnel. Do not edit. Run `make features`. -->\n\n\
         # `{feature}` footprint\n\n\
         The lines outside the `features/{feature}/` directories that name `{name}`:\n{list}\n"
    )
}
