use std::path::PathBuf;
use std::process::Command;

use cucumber::{given, then, World};

#[derive(Debug, Default, World)]
pub struct VirtuusWorld {
    pub version_file: Option<PathBuf>,
    pub python_version: Option<String>,
    pub rust_version: Option<String>,
}

// ---------------------------------------------------------------------------
// Scenario: Library loads and reports version
// ---------------------------------------------------------------------------

#[given("the virtuus library is available")]
async fn given_library_available(_world: &mut VirtuusWorld) {
    // The Rust library is always available since we compiled it.
}

#[then("it should report a valid version string")]
async fn then_valid_version(_world: &mut VirtuusWorld) {
    let v = virtuus::version();
    assert!(!v.is_empty(), "version() returned an empty string");
}

// ---------------------------------------------------------------------------
// Scenario: Version is read from the shared VERSION file
// ---------------------------------------------------------------------------

#[given("a VERSION file at the repository root")]
async fn given_version_file(world: &mut VirtuusWorld) {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let version_file = manifest_dir.parent().unwrap().join("VERSION");
    assert!(
        version_file.exists(),
        "VERSION file not found at {}",
        version_file.display()
    );
    world.version_file = Some(version_file);
}

#[then("the library version should match the contents of that file")]
async fn then_version_matches_file(world: &mut VirtuusWorld) {
    let path = world.version_file.as_ref().unwrap();
    let expected = std::fs::read_to_string(path).unwrap().trim().to_string();
    let actual = virtuus::VERSION;
    assert_eq!(
        actual, expected,
        "Library VERSION {actual:?} != VERSION file {expected:?}"
    );
}

// ---------------------------------------------------------------------------
// Scenario: Python and Rust report the same version
// ---------------------------------------------------------------------------

#[given("the Python virtuus library is available")]
async fn given_python_library(world: &mut VirtuusWorld) {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let python_src = manifest_dir.parent().unwrap().join("python").join("src");
    // {:?} gives a Rust debug-quoted string (e.g. "/path/to/src"), which is
    // a valid Python string literal.
    let src = python_src.to_str().unwrap();
    let script = format!(
        "import sys; sys.path.insert(0, {src:?}); import virtuus; print(virtuus.__version__)"
    );
    let output = Command::new("python3")
        .args(["-c", &script])
        .output()
        .expect("Failed to run python3 — is it on PATH?");
    assert!(
        output.status.success(),
        "Python command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    world.python_version = Some(String::from_utf8_lossy(&output.stdout).trim().to_string());
}

#[given("the Rust virtuus binary is available")]
async fn given_rust_binary(world: &mut VirtuusWorld) {
    // Use the library version directly (the binary reports the same value).
    world.rust_version = Some(virtuus::VERSION.to_string());
}

#[then("both should report the same version string")]
async fn then_same_version(world: &mut VirtuusWorld) {
    let py = world.python_version.as_deref().unwrap_or("");
    let rs = world.rust_version.as_deref().unwrap_or("");
    assert_eq!(py, rs, "Python version {py:?} != Rust version {rs:?}");
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    VirtuusWorld::run("../features").await;
}
