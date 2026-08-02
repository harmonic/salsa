use std::process::Command;

fn main() {
    // Best-effort: embed the git commit hash for `--version` reporting.
    // This must not fail the build when git or a .git directory isn't
    // available (e.g. builds from a source tarball, some Docker
    // multi-stage builds, or environments without git installed).
    let commit_hash = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=GIT_COMMIT_HASH={commit_hash}");

    // Without these, Cargo only reruns this script when a file inside
    // `harmonic-scheduler/` changes, so a commit that only touches other
    // crates in the workspace would leave GIT_COMMIT_HASH stale. Explicitly
    // watch HEAD and the ref it points to so the embedded hash always
    // reflects the actual commit being built.
    let git_dir = std::path::Path::new("..").join(".git");
    let head_path = git_dir.join("HEAD");
    if let Ok(head) = std::fs::read_to_string(&head_path) {
        println!("cargo:rerun-if-changed={}", head_path.display());
        if let Some(ref_path) = head.trim().strip_prefix("ref: ") {
            println!("cargo:rerun-if-changed={}", git_dir.join(ref_path).display());
        }
    }
}
