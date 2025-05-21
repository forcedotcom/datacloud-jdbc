# Spark datasource for Hyper

This Spark datasource allows reading results of Hyper queries inside your Spark jobs.

The main entry point of this data source is the `com.salesforce.datacloud.spark.HyperResultSource` class.

You can 

```
spark.read
  .format("com.salesforce.datacloud.spark.HyperResultSource")
  .option("query_id", queryId)
  .option("port", 8585)
  .load()
```

The `query_id` option indicates a query id acquired, e.g., via the
`DataCloudStatement.getQueryId()` function from the Datacloud JDBC driver.
All other options are identical to the JDBC connection options.

The Spark source uses Hyper's chunked results for distributed
processing, fetching to fetch different chunks on different Spark
workers. Furthermore, the driver provides custom metrics (row count and
chunk count) which can be observed in Spark's metric framework.
