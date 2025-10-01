## Development Guidelines

### Best Practices

#### Working with Streams

**FluentIterable** from Google Guava is the established reference interface for operating on iterator and stream-like data structures in this codebase. It provides a fluent API for transforming, filtering, and manipulating collections in a functional programming style.

## Coverage

Run `./gradlew clean build testCodeCoverageReport` to build the coverage report in `build/reports/jacoco/testCodeCoverageReport/html/index.html`.

There will also be an accompanying CSV in `build/reports/jacoco/testCodeCoverageReport/testCodeCoverageReport.csv` and a xml file in `build/reports/jacoco/testCodeCoverageReport/testCodeCoverageReport.xml` to be consumed by other tools.

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
  :spark-datasource --> :jdbc
  :spark-datasource --> :jdbc-grpc
  :spark-datasource --> :jdbc-core
  :spark-datasource --> :jdbc-util
  :jdbc-http --> :jdbc-util
  :jdbc --> :jdbc-core
  :jdbc --> :jdbc-util
  :jdbc --> :jdbc-http
  :jdbc --> :jdbc-grpc
```

Above graph is generated based on the gradle build files.
It can be regenerated using `./gradlew createModuleGraph`.