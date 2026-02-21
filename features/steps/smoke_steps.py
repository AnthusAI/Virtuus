import os
import subprocess

from behave import given, then  # noqa: F401


@given("the virtuus library is available")
def step_given_library_available(context):
    import virtuus

    context.virtuus = virtuus


@then("it should report a valid version string")
def step_then_valid_version(context):
    version = context.virtuus.__version__
    assert (
        isinstance(version, str) and len(version) > 0
    ), f"Expected non-empty version string, got {version!r}"


@given("a VERSION file at the repository root")
def step_given_version_file(context):
    repo_root = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
    context.version_file = os.path.join(repo_root, "VERSION")
    assert os.path.exists(
        context.version_file
    ), f"VERSION file not found at {context.version_file}"


@then("the library version should match the contents of that file")
def step_then_version_matches_file(context):
    import virtuus

    with open(context.version_file) as f:
        expected = f.read().strip()
    assert (
        virtuus.__version__ == expected
    ), f"Library version {virtuus.__version__!r} != VERSION file {expected!r}"


@given("the Python virtuus library is available")
def step_given_python_library(context):
    import virtuus

    context.python_version = virtuus.__version__


@given("the Rust virtuus binary is available")
def step_given_rust_binary(context):
    repo_root = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
    release_bin = os.path.join(repo_root, "rust", "target", "release", "virtuus")
    debug_bin = os.path.join(repo_root, "rust", "target", "debug", "virtuus")
    if os.path.exists(release_bin):
        context.rust_binary = release_bin
    elif os.path.exists(debug_bin):
        context.rust_binary = debug_bin
    else:
        raise AssertionError("Rust binary not found — run 'cargo build' in rust/ first")


@then("both should report the same version string")
def step_then_same_version(context):
    result = subprocess.run(
        [context.rust_binary, "--version"],
        capture_output=True,
        text=True,
        check=True,
    )
    # clap outputs "virtuus 0.1.0" on stdout
    rust_version = result.stdout.strip().split()[-1]
    assert (
        context.python_version == rust_version
    ), f"Python version {context.python_version!r} != Rust version {rust_version!r}"
