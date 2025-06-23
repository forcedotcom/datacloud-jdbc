package com.salesforce.datacloud.spark

import org.apache.spark.sql.types.{
  StructType,
  StructField,
  DataType,
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
import java.sql.Types
import org.apache.spark.sql.types.BinaryType
import java.sql.ResultSet
import org.apache.spark.sql.catalyst.InternalRow
import java.sql.ResultSetMetaData
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils

/** Type mapping utilities.
  *
  * Maps JDBC types to Spark types.
  */
object TypeMapping {

  /** Get the Spark type for a given JDBC type.
    *
    * @param md
    *   the metadata of the result set
    * @param columnId
    *   the column id, starting from 1
    * @return
    *   the Spark type
    */
  private def getSparkType(
      md: ResultSetMetaData,
      columnId: Int
  ): Option[DataType] = {
    md.getColumnType(columnId) match {
      // See
      //   https://github.com/forcedotcom/datacloud-jdbc/blob/dff10a6be48af2ed3d814fd7ce7f10d640ecf5f3/jdbc-core/src/main/java/com/salesforce/datacloud/jdbc/util/ArrowUtils.java#L227
      //   https://github.com/apache/spark/blob/633419a8c7a342f7cd93f84e1241adee1e1195f0/sql/core/src/main/scala/org/apache/spark/sql/jdbc/PostgresDialect.scala#L57
      case Types.BOOLEAN  => Some(BooleanType)
      case Types.SMALLINT => Some(ShortType)
      case Types.INTEGER  => Some(IntegerType)
      case Types.BIGINT   => Some(LongType)
      case Types.DECIMAL =>
        Some(
          DecimalType(
            md.getPrecision(columnId),
            md.getScale(columnId)
          )
        )
      case Types.REAL   => Some(FloatType)
      case Types.DOUBLE => Some(DoubleType)
      case Types.DATE   => Some(DateType)
      // TODO: distinguish between Timestamp and TimestampTZ
      case Types.TIMESTAMP     => Some(TimestampType)
      case Types.VARCHAR       => Some(StringType)
      case Types.LONGVARCHAR   => Some(StringType)
      case Types.BINARY        => Some(BinaryType)
      case Types.VARBINARY     => Some(BinaryType)
      case Types.LONGVARBINARY => Some(BinaryType)
      case _                   => None
    }
  }

  /** Get the Spark types for all columns in the result set. */
  def getSparkFields(
      md: ResultSetMetaData
  ): StructType = {
    val columns = Array.tabulate(md.getColumnCount()) { i =>
      val columnName = md.getColumnLabel(i + 1)
      val columnType = TypeMapping.getSparkType(md, i + 1)
      if (!columnType.isDefined) {
        throw new IllegalArgumentException(
          s"Unsupported column type ${md.getColumnTypeName(i + 1)} for column $columnName"
        )
      }
      val mightBeNull =
        md.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
      StructField(columnName, columnType.get, mightBeNull)
    }

    StructType(columns)
  }

  /** A `JDBCValueGetter` is responsible for getting a value from `ResultSet`
    * into a field for `MutableRow`. The last argument `Int` means the index for
    * the value to be set in the row and also used for the value in `ResultSet`.
    */
  type JDBCValueGetter = (ResultSet, InternalRow, Int) => Unit

  private def nullSafeConvert[T](input: T, f: T => Any): Any = {
    if (input == null) {
      null
    } else {
      f(input)
    }
  }

  /** Make a `JDBCValueGetter` for a given column id. */
  private def makeGetter(
      md: ResultSetMetaData,
      columnId: Int
  ): JDBCValueGetter = {
    md.getColumnType(columnId + 1) match {
      // See https://github.com/apache/spark/blob/0fb544583f0c4bfd802bec8eca1fab32cd9031f8/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L406
      case Types.BOOLEAN =>
        (rs: ResultSet, row: InternalRow, pos: Int) =>
          row.setBoolean(pos, rs.getBoolean(pos + 1))
      case Types.SMALLINT =>
        (rs: ResultSet, row: InternalRow, pos: Int) =>
          row.setShort(pos, rs.getShort(pos + 1))
      case Types.INTEGER =>
        (rs: ResultSet, row: InternalRow, pos: Int) =>
          row.setInt(pos, rs.getInt(pos + 1))
      case Types.BIGINT =>
        (rs: ResultSet, row: InternalRow, pos: Int) =>
          row.setLong(pos, rs.getLong(pos + 1))
      case Types.DECIMAL =>
        (rs: ResultSet, row: InternalRow, pos: Int) =>
          row.update(
            pos,
            nullSafeConvert(rs.getBigDecimal(pos + 1), Decimal.fromDecimal)
          )
      case Types.REAL =>
        (rs: ResultSet, row: InternalRow, pos: Int) =>
          row.setFloat(pos, rs.getFloat(pos + 1))
      case Types.DOUBLE =>
        (rs: ResultSet, row: InternalRow, pos: Int) =>
          row.setDouble(pos, rs.getDouble(pos + 1))
      case Types.DATE =>
        (rs: ResultSet, row: InternalRow, pos: Int) =>
          row.update(
            pos,
            nullSafeConvert(
              rs.getDate(pos + 1),
              SparkDateTimeUtils.fromJavaDate
            )
          )
      case Types.TIMESTAMP =>
        (rs: ResultSet, row: InternalRow, pos: Int) =>
          row.update(
            pos,
            nullSafeConvert(
              rs.getTimestamp(pos + 1),
              SparkDateTimeUtils.fromJavaTimestamp
            )
          )
      case Types.VARCHAR | Types.LONGVARCHAR =>
        (rs: ResultSet, row: InternalRow, pos: Int) =>
          // We use getBytes for better performance, to avoid encoding UTF-8 to Java's UTF-16.
          row.update(
            pos,
            UTF8String.fromBytes(rs.getBytes(pos + 1))
          )
      case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY =>
        (rs: ResultSet, row: InternalRow, pos: Int) =>
          row.update(pos, rs.getBytes(pos + 1))
    }
  }

  /** Make a `JDBCValueGetter` for each column in the result set. */
  def makeGetters(
      md: ResultSetMetaData
  ): Array[JDBCValueGetter] =
    Array.tabulate(md.getColumnCount())(makeGetter(md, _))
}
