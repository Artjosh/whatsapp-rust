//! Keep the independently resolved, minimal-feature host fixture in the normal
//! integration-test gate without adding a second workflow or workspace member.

use std::process::Command;

#[test]
fn standalone_consumer_without_workspace_codegen_flags() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = root.join("tests/api-a01-consumer/Cargo.toml");
    // A separate target avoids recursively waiting on the parent Cargo build's
    // lock and keeps feature unification out of the consumer's compilation.
    let parent = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .map_or_else(|| root.join("target"), |path| root.join(path));
    std::fs::create_dir_all(&parent).expect("parent build directory");
    let target = tempfile::Builder::new()
        .prefix("api-a01-consumer-")
        .tempdir_in(parent)
        .expect("consumer build directory");
    let output = Command::new(env!("CARGO"))
        .arg("test")
        .arg("--manifest-path")
        .arg(&manifest)
        .current_dir(manifest.parent().expect("fixture directory"))
        .env("CARGO_TARGET_DIR", target.path())
        .env("CARGO_BUILD_JOBS", "2")
        // A set-but-empty RUSTFLAGS overrides the repository's nightly-only
        // codegen configuration, just as the MSRV workflow does.
        .env("RUSTFLAGS", "")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .output()
        .expect("run standalone consumer tests and doctests");
    assert!(
        output.status.success(),
        "standalone consumer failed ({}):\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
