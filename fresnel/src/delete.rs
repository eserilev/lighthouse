//! Text edits of `make delete-feature`.

/// Remove the `[feature]` table from the registry source, up to the next table.
pub fn remove_registry_table(source: &str, feature: &str) -> String {
    let header = format!("[{feature}]");
    let mut in_table = false;
    let mut out: Vec<&str> = vec![];
    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            in_table = trimmed == header;
        }
        if !in_table {
            out.push(line);
        }
    }
    while out.last().is_some_and(|line| line.trim().is_empty()) {
        out.pop();
    }
    lines_to_source(&out)
}

/// Remove each `#[feature_dispatch(name => ...)]` attribute, on one or more lines.
pub fn remove_dispatch_attributes(source: &str, name: &str) -> String {
    let mut out: Vec<&str> = vec![];
    let mut attribute: Vec<&str> = vec![];
    for line in source.lines() {
        if attribute.is_empty() && !line.trim_start().starts_with("#[feature_dispatch(") {
            out.push(line);
            continue;
        }
        attribute.push(line);
        if line.contains(")]") {
            let text = attribute.concat();
            let dispatched = text
                .split_once("#[feature_dispatch(")
                .and_then(|(_, args)| args.split_once("=>"))
                .map(|(feature, _)| feature.trim());
            if dispatched != Some(name) {
                out.append(&mut attribute);
            }
            attribute.clear();
        }
    }
    out.append(&mut attribute);
    lines_to_source(&out)
}

/// Remove each `mod feature;` declaration, with any visibility.
pub fn remove_mod_declarations(source: &str, feature: &str) -> String {
    let declaration = format!("mod {feature};");
    let out: Vec<&str> = source
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            trimmed != declaration
                && !(trimmed.starts_with("pub") && trimmed.ends_with(&format!(" {declaration}")))
        })
        .collect();
    lines_to_source(&out)
}

fn lines_to_source(lines: &[&str]) -> String {
    let mut source = lines.join("\n");
    source.push('\n');
    source
}
