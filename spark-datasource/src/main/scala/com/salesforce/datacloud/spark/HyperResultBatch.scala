package com.salesforce.datacloud.spark

import java.sql.ResultSet
import com.salesforce.datacloud.jdbc.core.DataCloudConnection
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.{
  StructType,
  DataType,
  StructField,
  BooleanType,
  ByteType,
  ShortType,
  IntegerType,
  LongType,
  FloatType,
  DoubleType,
  StringType,
  DateType,
  TimestampType,
  DecimalType
}
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.connector.read.Batch
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.catalyst.InternalRow
import com.salesforce.datacloud.spark.TypeMapping.JDBCValueGetter
import com.salesforce.datacloud.spark.TypeMapping.makeGetters

/** A partition of a Hyper result.
  *
  * Each partition is one or multiple chunks of a Hyper result.
  */
private case class HyperResultInputPartition(
    resultSetId: String,
    chunkIndex: Long,
    chunkCount: Long
) extends InputPartition {}

/** A batch is a collection of partitions.
  */
private case class HyperResultBatch(
    connectionOptions: HyperConnectionOptions,
    resultSetId: String,
    schema: StructType,
    chunkCount: Long
) extends Batch {
  override def planInputPartitions(): Array[InputPartition] = {
    // Try to create 512 partitions, but not more than chunkCount
    val partitionCount = Math.min(chunkCount.toInt, 512)
    Array.tabulate(partitionCount)(i => {
      // Calculate the number of chunks in the current partition.
      var chunksPerPartition = chunkCount / partitionCount
      if (i < chunkCount % partitionCount) {
        // Handle rounding issues with integer division.
        chunksPerPartition += 1
      }
      HyperResultInputPartition(resultSetId, i, chunksPerPartition)
    })
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new PartitionReaderFactory {
      override def createReader(
          partitionBase: InputPartition
      ): PartitionReader[InternalRow] = {
        val partition = partitionBase.asInstanceOf[HyperResultInputPartition]
        new HyperResultPartitionReader(
          schema,
          connectionOptions,
          resultSetId,
          partition.chunkIndex,
          partition.chunkCount
        )
      }
    }
  }
}

private class HyperResultPartitionReader extends PartitionReader[InternalRow] {
  private var getters: Array[JDBCValueGetter] = null;
  private var connection: DataCloudConnection = null;
  private var resultSet: ResultSet = null;
  private var mutableRow: SpecificInternalRow = null;

  def this(
      schema: StructType,
      connectionOptions: HyperConnectionOptions,
      resultSetId: String,
      chunkIndex: Long,
      chunkCount: Long
  ) = {
    this()
    mutableRow = new SpecificInternalRow(
      schema.fields.map(x => x.dataType).toIndexedSeq
    )
    connection = connectionOptions.createConnection()
    resultSet = this.connection.getChunkBasedResultSet(
      resultSetId,
      chunkIndex,
      chunkCount
    )
    val metadata = resultSet.getMetaData();
    assert(metadata.getColumnCount() == schema.length)
    this.getters = makeGetters(metadata)
  }

  override def next(): Boolean = resultSet.next()

  override def get(): InternalRow = {
    // See https://github.com/apache/spark/blob/d5f735b54a4d0cb87d027f6b1100160433d5f599/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L353
    for (i <- 0 until getters.length) {
      getters(i).apply(resultSet, mutableRow, i)
      if (resultSet.wasNull) mutableRow.setNullAt(i)
    }
    mutableRow
  }

  override def close(): Unit = {
    resultSet.close()
    connection.close()
  }
}
