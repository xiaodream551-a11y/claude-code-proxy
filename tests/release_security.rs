const RELEASE_WORKFLOW: &str = include_str!("../.github/workflows/release.yml");
const NIX_WORKFLOW: &str = include_str!("../.github/workflows/nix.yml");

#[test]
fn third_party_actions_are_pinned_to_full_commit_shas() {
    for (workflow_name, workflow) in [("release.yml", RELEASE_WORKFLOW), ("nix.yml", NIX_WORKFLOW)]
    {
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
    assert!(RELEASE_WORKFLOW.contains("permissions:\n  contents: read"));
    assert_eq!(RELEASE_WORKFLOW.matches("contents: write").count(), 1);
    assert!(
        RELEASE_WORKFLOW
            .contains("contents: write\n      id-token: write\n      attestations: write")
    );
    assert!(RELEASE_WORKFLOW.contains("persist-credentials: false"));
    assert!(RELEASE_WORKFLOW.contains("Verify release immutability"));
    assert!(RELEASE_WORKFLOW.contains("repos/$GITHUB_REPOSITORY/releases/tags/$GITHUB_REF_NAME"));
    assert!(RELEASE_WORKFLOW.contains("--jq '.immutable'"));
    assert!(!RELEASE_WORKFLOW.contains("repos/$GITHUB_REPOSITORY/immutable-releases"));
    assert!(RELEASE_WORKFLOW.contains("uses: actions/attest@"));
    assert!(RELEASE_WORKFLOW.contains("gh release create"));
    assert!(!RELEASE_WORKFLOW.contains("softprops/action-gh-release"));
}
