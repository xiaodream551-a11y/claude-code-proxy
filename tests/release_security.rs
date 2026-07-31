const RELEASE_WORKFLOW: &str = include_str!("../.github/workflows/release.yml");
const NIX_WORKFLOW: &str = include_str!("../.github/workflows/nix.yml");

fn normalize_newlines(source: &str) -> String {
    // Git for Windows may materialize tracked text files with CRLF. Normalize
    // before content assertions so the least-privilege contract is checked for
    // semantics rather than checkout line endings.
    source.replace("\r\n", "\n").replace('\r', "\n")
}

#[test]
fn third_party_actions_are_pinned_to_full_commit_shas() {
    for (workflow_name, workflow) in [
        ("release.yml", normalize_newlines(RELEASE_WORKFLOW)),
        ("nix.yml", normalize_newlines(NIX_WORKFLOW)),
    ] {
        for line in workflow.lines() {
            let trimmed = line.trim();
            let Some(reference) = trimmed.strip_prefix("- uses: ") else {
                continue;
            };
            let Some((_, revision_and_comment)) = reference.split_once('@') else {
                panic!("{workflow_name}: action reference has no revision: {trimmed}");
            };
            let revision = revision_and_comment
                .split_ascii_whitespace()
                .next()
                .unwrap_or_default();
            assert_eq!(
                revision.len(),
                40,
                "{workflow_name}: action is not pinned to a full commit SHA: {trimmed}"
            );
            assert!(
                revision.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "{workflow_name}: action revision is not hexadecimal: {trimmed}"
            );
        }
    }
}

#[test]
fn release_workflow_uses_least_privilege_and_signed_provenance() {
    let release_workflow = normalize_newlines(RELEASE_WORKFLOW);
    assert!(release_workflow.contains("permissions:\n  contents: read"));
    assert_eq!(release_workflow.matches("contents: write").count(), 1);
    assert!(
        release_workflow
            .contains("contents: write\n      id-token: write\n      attestations: write")
    );
    assert!(release_workflow.contains("persist-credentials: false"));
    assert!(release_workflow.contains("Verify release immutability"));
    assert!(release_workflow.contains("repos/$GITHUB_REPOSITORY/releases/tags/$GITHUB_REF_NAME"));
    assert!(release_workflow.contains("--jq '.immutable'"));
    assert!(!release_workflow.contains("repos/$GITHUB_REPOSITORY/immutable-releases"));
    assert!(release_workflow.contains("uses: actions/attest@"));
    assert!(release_workflow.contains("gh release create"));
    assert!(!release_workflow.contains("softprops/action-gh-release"));
}
