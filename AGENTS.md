# Agent Instructions

## Project management with Kanbus

Use Kanbus for task management.
Why: Kanbus task management is MANDATORY here; every task must live in Kanbus.
When: Create/update the Kanbus task before coding; close it only after the change lands.
How: See CONTRIBUTING_AGENT.md for the Kanbus workflow, hierarchy, status rules, priorities, command examples, and the sins to avoid. Never inspect project/ or issue JSON directly (including with cat or jq); use Kanbus commands only.
Performance: Prefer kanbusr (Rust) when available; kanbus (Python) is equivalent but slower.
Warning: Editing project/ directly is a sin against The Way. Do not read or write anything in project/; work only through Kanbus.
Git workflow: This repo uses Git Flow. Do development work on `dev` and only merge to `main` for releases.

## Code quality standards

### Python

- **Black + Ruff compliance is mandatory.** All Python source code must pass `python3 -m black --check` and `python3 -m ruff check` before merging.
- **Sphinx-style docstrings** are required for all public functions, classes, and modules in `python/src/`. Use reStructuredText field lists: `:param`, `:type`, `:return:`, `:rtype:`, `:raises:`.
- **No line-level comments.** Use descriptive names and small, readable functions instead. Block comments are acceptable only for capturing high-level rationale.
- **Docstring linting** is enforced via Ruff's `D` rule set (pydocstyle). All public symbols must have docstrings; missing docstrings are a lint error.
- **100% test coverage is required.** All production code in `python/src/` must be covered by BDD/behavior specifications. `make coverage-python` must report 100% or the build fails.

### Rust

- **`cargo fmt --check` and `cargo clippy -- -D warnings`** must both pass. All warnings are treated as errors.
- **Doc comments (`///`)** are required for all public functions, structs, enums, and constants in `rust/src/`. Use standard Rust doc comment conventions.
- **100% test coverage is required.** All production code in `rust/src/lib.rs` (and any future modules) must be covered. `make coverage-rust` must report 100% or the build fails. Entry points (`src/bin/`) are excluded from coverage measurement.

### General

- **`make check` must be green before any commit.** This runs lint, specs, coverage (both languages), and the parity check in one command.
- **Parity is enforced.** Every Gherkin step implemented in Python must also be implemented in Rust, and vice versa. `make check-parity` fails on asymmetry.
- **Specs before code.** See CONTRIBUTING_AGENT.md — production code exists only to make a failing specification pass.

### Example: Sphinx docstring

```python
def put(self, record: dict) -> None:
    """
    Insert or update a record in the table.

    :param record: The record to store. Must contain the primary key field.
    :type record: dict
    :return: None
    :rtype: None
    :raises ValueError: If the record is missing the primary key field.
    """
```
