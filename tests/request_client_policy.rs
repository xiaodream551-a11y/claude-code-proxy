use std::path::{Path, PathBuf};

fn rust_sources_under(root: &Path, output: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(root).expect("provider source directory must be readable") {
        let path = entry.expect("source entry must be readable").path();
        if path.is_dir() {
            rust_sources_under(&path, output);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
}

#[test]
fn codex_and_grok_reqwest_clients_disable_library_retries() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    for provider in ["codex", "grok"] {
        rust_sources_under(&manifest.join("src/providers").join(provider), &mut sources);
    }
    sources.push(manifest.join("src/upstream_http.rs"));

    let mut builders = 0;
    for path in sources {
        let source = std::fs::read_to_string(&path).expect("Rust source must be UTF-8");
        assert!(
            !source.contains("reqwest::Client::new()")
                && !source.contains("reqwest::blocking::Client::new()"),
            "{} constructs a Reqwest client without an explicit retry policy",
            path.display()
        );

        let lines: Vec<_> = source.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if !line.contains("Client::builder()") {
                continue;
            }
            builders += 1;
            let end = (index + 12).min(lines.len());
            let builder_chain = lines[index..end].join("\n");
            assert!(
                builder_chain.contains(".retry(reqwest::retry::never())"),
                "{}:{} does not disable Reqwest's protocol-level retries",
                path.display(),
                index + 1
            );
        }
    }

    assert!(
        builders >= 8,
        "expected all production client builders to be scanned"
    );
}
