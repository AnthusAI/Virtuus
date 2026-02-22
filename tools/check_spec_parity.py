"""
Verify that every Gherkin step has implementations in both Python and Rust.

Python steps:  features/steps/**/*.py
Rust steps:    rust/tests/**/*.rs
"""

import re
import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent

# Gherkin step keywords (normalised to step text only)
STEP_KEYWORDS = ("Given ", "When ", "Then ", "And ", "But ")

# Python: @given("..."), @when("..."), @then("..."), @step("...")
# Also handles u"..." and u'...' string prefixes.
PYTHON_PATTERN = re.compile(
    r'@(?:given|when|then|step)\s*\(\s*(?:u|r|ur|ru)?["\'](.+?)["\']\s*\)',
    re.IGNORECASE,
)

# Rust: #[given("...")], #[when("...")], #[then("...")]
# Handles both plain strings and regex = r#"..."# raw string syntax.
# Two patterns: one for raw strings r#"..."#, one for plain strings.
RUST_PATTERN_RAW = re.compile(
    r'#\[\s*(?:given|when|then)\s*\([^)]*?regex\s*=\s*r#"(.+?)"#\s*\)\s*\]',
    re.IGNORECASE | re.DOTALL,
)
RUST_PATTERN_PLAIN = re.compile(
    r'#\[\s*(?:given|when|then)\s*\(\s*("(?:(?:[^"\\]|\\.)*)")\s*\)\s*\]',
    re.IGNORECASE,
)


def extract_feature_steps() -> list[str]:
    steps: list[str] = []
    for path in sorted(REPO_ROOT.glob("features/**/*.feature")):
        lines = path.read_text().splitlines()
        # Skip python-only features from parity enforcement.
        # If a tag line preceding the Feature contains "python-only",
        # we treat the whole file as out-of-scope for Rust parity.
        before_feature = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("Feature:"):
                break
            if stripped:
                before_feature.append(stripped)
        if any("python-only" in tag for tag in before_feature):
            continue

        for line in lines:
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
    """Extract step patterns from Rust cucumber test files.

    :return: List of step pattern strings (plain literals and raw regexes).
    :rtype: list[str]
    """
    patterns: list[str] = []
    for path in sorted(REPO_ROOT.glob("rust/tests/**/*.rs")):
        text = path.read_text()
        patterns.extend(RUST_PATTERN_RAW.findall(text))
        for literal in RUST_PATTERN_PLAIN.findall(text):
            try:
                patterns.append(ast.literal_eval(literal))
            except Exception:
                patterns.append(literal.strip('"'))
    return patterns


def _to_regex(pattern: str) -> re.Pattern:
    """Convert a step pattern to a regex, handling {param} placeholders.

    If the pattern already looks like a regex (starts with ``^``), it is
    used as-is.  Otherwise it is treated as a behave/cucumber literal pattern
    with ``{param}`` placeholders and quoted-value wildcards.

    :param pattern: The raw step pattern string.
    :type pattern: str
    :return: Compiled regular expression.
    :rtype: re.Pattern
    """
    if (
        pattern.startswith("^")
        or re.search(r"\(\[\^", pattern)
        or re.search(r"\(\\\{", pattern)
        or re.search(r"\(\.\*", pattern)
    ):
        return re.compile(pattern, re.IGNORECASE)
    pattern = pattern.replace(r"\"", '"')
    escaped = re.escape(pattern)
    # {param:d} → \d+ (numeric constraints)
    escaped = re.sub(r"\\\{[^}:]+:d\\\}", r"\\d+", escaped)
    # {param} → .*  (behave/cucumber style)
    escaped = re.sub(r"\\\{[^}]+\\\}", ".*", escaped)
    # "quoted value" inside a pattern → match any quoted value
    escaped = re.sub(r'"[^"]*"', '"[^"]*"', escaped)
    return re.compile(f"^{escaped}$", re.IGNORECASE)


def matches_any(step: str, patterns: list[str]) -> bool:
    """Return True if *step* matches any pattern in *patterns*.

    :param step: The Gherkin step text to test.
    :type step: str
    :param patterns: List of step patterns from Python or Rust implementations.
    :type patterns: list[str]
    :return: True if a match is found.
    :rtype: bool
    """
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

    # Normalize Gherkin Scenario Outline placeholders <param> → {param}
    # so they match Python {param} patterns and Rust regex captures.
    # Skip steps that still contain {param} placeholders after normalization —
    # those are abstract outline forms; the concrete expanded forms are what matter.
    normalized: set[str] = set()
    for s in steps:
        n = re.sub(r"<([^>]+)>", r"{\1}", s)
        if "{" not in n:
            normalized.add(n)
    unique_steps = sorted(normalized)
    python_only: list[str] = []  # implemented in Python but not Rust
    rust_only: list[str] = []  # implemented in Rust but not Python

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
        1
        for s in unique_steps
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
