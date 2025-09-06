# Spark datasource for Hyper

This Spark datasource allows reading results of Hyper queries inside your Spark jobs.

The main entry point of this data source is the `com.salesforce.datacloud.spark.HyperResultSource` class.

You can use

```
spark.read
  .format("com.salesforce.datacloud.spark.HyperResultSource")
  .option("jdbcUrl", "jdbc:salesforce-hyper://localhost:7483")
  .option("workload", "spark-test")
  .option("queryId", queryId)
  .load()
```

The `queryId` option indicates a query id acquired, e.g., via the
`DataCloudStatement.getQueryId()` function from the Datacloud JDBC driver.
All other options are identical to the JDBC connection options.

The `jdbcUrl` contains the endpoint used to connect to Hyper.
You can use either `jdbc:salesforce-hyper` or `jdbc:salesforce-datacloud`
URLs. All other properties are passed through to the JDBC driver.

The Spark source uses Hyper's chunked results for distributed
processing, fetching to fetch different chunks on different Spark
workers. Furthermore, the driver provides custom metrics (row count and
chunk count) which can be observed in Spark's metric framework.
