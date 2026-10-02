@rust-only
Feature: Rust-only library features
  As a developer building with Rust
  I want to verify Rust-specific compilation modes
  So that the library can be used in WebAssembly and other constrained environments

  Scenario: Library builds without default features
    Given the rust library can be compiled
    Then it should build without default features
