package com.salesforce.datacloud.spark

import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.types.StructType
import java.{util => ju}
import java.time.Duration
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.read.SupportsReportStatistics
import org.apache.spark.sql.connector.read.Batch
import org.apache.spark.sql.connector.read.Statistics
import scala.util.Using
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.metric.CustomTaskMetric

private case class HyperResultTable(
    connectionOptions: HyperConnectionOptions,
    resultSetId: String,
    schema: StructType
) extends SupportsRead {
  override def name(): String = s"hyper_result_set_${resultSetId}"

  override def capabilities(): ju.Set[TableCapability] = {
    val capabilities = new ju.HashSet[TableCapability]()
    capabilities.add(TableCapability.BATCH_READ)
    capabilities
  }

  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder = {
    val (chunkCount, rowCount) =
      Using(connectionOptions.createConnection()) { conn =>
        // TODO XXX: set infinite timeout
        val queryStatus =
          conn.waitForResultsProduced(resultSetId, Duration.ofDays(2))
        (queryStatus.getChunkCount(), queryStatus.getRowCount())
      }.get

    new ScanBuilder {
      override def build(): Scan = HyperResultScan(
        connectionOptions,
        resultSetId,
        schema,
        chunkCount,
        rowCount
      )
    }
  }
}

private case class HyperResultScan(
    connectionOptions: HyperConnectionOptions,
    resultSetId: String,
    schema: StructType,
    chunkCount: Long,
    rowCount: Long
) extends Scan
    with SupportsReportStatistics {
  override def estimateStatistics(): Statistics = new Statistics {
    override def sizeInBytes(): ju.OptionalLong =
      ju.OptionalLong.empty()
    override def numRows(): ju.OptionalLong =
      ju.OptionalLong.of(rowCount)
  }

  override def supportedCustomMetrics(): Array[CustomMetric] = {
    Array(new ChunkCountMetric)
  }

  override def reportDriverMetrics(): Array[CustomTaskMetric] = {
    Array(
      new ChunkCountTaskMetric(chunkCount)
    )
  }

  override def readSchema(): StructType = schema
  override def toBatch(): Batch = new HyperResultBatch(
    connectionOptions,
    resultSetId,
    schema,
    chunkCount
  )
}

class ChunkCountMetric extends CustomMetric {
  override def name(): String = "hyper_result_chunk_count"
  override def description(): String =
    "number of chunks in the result set from Hyper"
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    taskMetrics.sum.toString
  }
}

class ChunkCountTaskMetric(value: Long) extends CustomTaskMetric {
  override def name(): String = "hyper_result_chunk_count"
  override def value(): Long = value
}
