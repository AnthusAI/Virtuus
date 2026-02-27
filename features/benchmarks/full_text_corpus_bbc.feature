@python-only @bench
Feature: BBC News corpus fixtures
  As a developer measuring search performance
  I want a realistic news corpus
  So that full-text benchmarks are meaningful

  Scenario: BBC News fixtures can be generated
    Given the "bbc_news_alltime" fixture profile at scale factor 1
    When I generate fixtures
    Then the "news" directory should contain at least 10000 JSON files
