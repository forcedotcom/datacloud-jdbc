## Development Guidelines

### Best Practices

#### Working with Streams and Iterators

**FluentIterable** from Google Guava is the established reference interface for operating on iterator and stream-like data structures in this codebase. It provides a fluent API for transforming, filtering, and manipulating collections in a functional programming style.

While we also considered the Java Streams API, we discourage its use because the `Spliterator` concept together with `tryAdvance` can sneakily lead to OOMs due to materializing full Iterators.

## Coverage

Run `./gradlew clean build testCodeCoverageReport` to build the coverage report in `build/reports/jacoco/testCodeCoverageReport/html/index.html`.

There will also be an accompanying CSV in `build/reports/jacoco/testCodeCoverageReport/testCodeCoverageReport.csv` and a xml file in `build/reports/jacoco/testCodeCoverageReport/testCodeCoverageReport.xml` to be consumed by other tools.

## Maintenance

The following commands should be run regularly to ensure the project stays healthy
- `./gradlew createModuleGraph`, it updates the module graph in this document and should be run every time the inter project dependencies change
- `./gradlew versionCatalogUpdate`, should be run every time a dependency is added and regularly anyway to avoid vulnerable versions

## Module Graph

```mermaid
%%{
  init: {
    'theme': 'neutral'
  }
}%%

graph LR
  :jdbc-core --> :jdbc-grpc
  :jdbc-core --> :jdbc-util
  :jdbc-core --> :jdbc-reference
  :spark-datasource --> :spark-datasource-core
  :spark-datasource --> :jdbc
  :spark-datasource-core --> :jdbc-grpc
  :spark-datasource-core --> :jdbc-core
  :spark-datasource-core --> :jdbc-util
  :jdbc-http --> :jdbc-util
  :jdbc --> :jdbc-core
  :jdbc --> :jdbc-util
  :jdbc --> :jdbc-http
  :jdbc --> :jdbc-grpc
```