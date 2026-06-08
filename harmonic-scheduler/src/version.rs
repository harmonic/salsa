/// Semantic version of the scheduler, from the crate's `CARGO_PKG_VERSION`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Git commit hash the binary was built from, injected by `build.rs`.
pub const COMMIT: &str = env!("GIT_COMMIT_HASH");

/// Full version reported by `--version`: semver with the git commit hash as build metadata.
pub const BUILD_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "+", env!("GIT_COMMIT_HASH"));
