use std::process::Command;

fn main() {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("failed to execute git");
    if !output.status.success() {
        panic!("failed to get git commit hash");
    }
    println!(
        "cargo:rustc-env=GIT_COMMIT_HASH={}",
        String::from_utf8(output.stdout).unwrap().trim()
    );
}
