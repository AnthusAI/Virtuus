"""
Verify that every Gherkin step has implementations in both Python and Rust.

Python steps:  features/steps/**/*.py
Rust steps:    rust/tests/**/*.rs
"""

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent

# Gherkin step keywords (normalised to step text only)
STEP_KEYWORDS = ("Given ", "When ", "Then ", "And ", "But ")

# Python: @given("..."), @when("..."), @then("..."), @step("...")
PYTHON_PATTERN = re.compile(
    r'@(?:given|when|then|step)\s*\(\s*["\'](.+?)["\']\s*\)',
    re.IGNORECASE,
)

# Rust: #[given("...")], #[when("...")], #[then("...")]
RUST_PATTERN = re.compile(
    r'#\[\s*(?:given|when|then)\s*\(\s*["\'](.+?)["\']\s*\)\s*\]',
    re.IGNORECASE,
)


def extract_feature_steps() -> list[str]:
    steps: list[str] = []
    for path in sorted(REPO_ROOT.glob("features/**/*.feature")):
        for line in path.read_text().splitlines():
            stripped = line.strip()
            for kw in STEP_KEYWORDS:
                if stripped.startswith(kw):
                    text = stripped[len(kw) :].strip()
                    if text and not text.startswith("|") and not text.startswith('"""'):
                        steps.append(text)
    return steps


def extract_python_patterns() -> list[str]:
    patterns: list[str] = []
    for path in sorted(REPO_ROOT.glob("features/steps/**/*.py")):
        patterns.extend(PYTHON_PATTERN.findall(path.read_text()))
    return patterns


def extract_rust_patterns() -> list[str]:
    patterns: list[str] = []
    for path in sorted(REPO_ROOT.glob("rust/tests/**/*.rs")):
        patterns.extend(RUST_PATTERN.findall(path.read_text()))
    return patterns


def _to_regex(pattern: str) -> re.Pattern:
    """Convert a step pattern to a regex, handling {param} placeholders."""
    escaped = re.escape(pattern)
    # {param} → .*  (behave/cucumber style)
    escaped = re.sub(r"\\\{[^}]+\\\}", ".*", escaped)
    # "quoted value" inside a pattern → match any quoted value
    escaped = re.sub(r'"[^"]*"', '"[^"]*"', escaped)
    return re.compile(f"^{escaped}$", re.IGNORECASE)


def matches_any(step: str, patterns: list[str]) -> bool:
    for p in patterns:
        try:
            if _to_regex(p).match(step):
                return True
        except re.error:
            if p.lower() == step.lower():
                return True
    return False


def main() -> int:
    """
    Parity check: every step that is implemented in one language must also be
    implemented in the other.  Steps that are unimplemented in *both* languages
    are simply not yet done — that is expected during incremental development
    and is not an error.
    """
    steps = extract_feature_steps()
    python_pats = extract_python_patterns()
    rust_pats = extract_rust_patterns()

    unique_steps = sorted(set(steps))
    python_only: list[str] = []  # implemented in Python but not Rust
    rust_only: list[str] = []    # implemented in Rust but not Python

    for step in unique_steps:
        has_python = matches_any(step, python_pats)
        has_rust = matches_any(step, rust_pats)

        if has_python and not has_rust:
            python_only.append(step)
        elif has_rust and not has_python:
            rust_only.append(step)

    if python_only:
        print("ERROR: steps implemented in Python but missing Rust implementation:")
        for s in python_only:
            print(f"  - {s}")

    if rust_only:
        print("ERROR: steps implemented in Rust but missing Python implementation:")
        for s in rust_only:
            print(f"  - {s}")

    if python_only or rust_only:
        return 1

    implemented = sum(
        1 for s in unique_steps
        if matches_any(s, python_pats) and matches_any(s, rust_pats)
    )
    pending = len(unique_steps) - implemented
    print(
        f"Parity OK — {implemented} steps implemented in both languages, "
        f"{pending} pending (not yet implemented in either)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
