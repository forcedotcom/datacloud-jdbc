import org.scalatest.funsuite.AnyFunSuite
import scala.util.Using
import com.salesforce.datacloud.jdbc.core.DataCloudStatement
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.sql.Date
import java.sql.Timestamp
import java.math.BigDecimal
import org.apache.spark.sql.types.{
  StructField,
  ShortType,
  IntegerType,
  LongType,
  BooleanType,
  DecimalType,
  FloatType,
  DoubleType,
  StringType,
  DateType,
  TimestampType,
  BinaryType
}

class HyperResultSourceTest
    extends AnyFunSuite
    with WithSparkSession
    with WithHyperServer {

  test("reports an error on missing query id") {
    val e = intercept[IllegalArgumentException] {
      spark.read
        .format("com.salesforce.datacloud.spark.HyperResultSource")
        .option("port", hyperServerProcess.getPort())
        .load()
    }
    assert(e.getMessage.equals("Missing `query_id` property"))
  }

  test("reports an error on invalid query id") {
    val e = intercept[DataCloudJDBCException] {
      spark.read
        .format("com.salesforce.datacloud.spark.HyperResultSource")
        .option("port", hyperServerProcess.getPort())
        .option("query_id", "invalid")
        .load()
    }
    assert(
      e.getMessage().contains("The requested query ID is unknown"),
      e.getMessage()
    )
  }

  test("supports reading all types") {
    val queryId = Using.Manager { use =>
      val connection = use(hyperServerProcess.getConnection());
      val stmt =
        use(connection.createStatement().unwrap(classOf[DataCloudStatement]))
      // TODO: Also test NULLs. This is currently not possible, because the JDBC driver
      // returns wrong nullability info.
      stmt.execute("""
        SELECT
          true::boolean AS boolean,
          1::smallint AS smallint,
          2::int AS int,
          3::bigint AS bigint,
          1.23::decimal(10,2) AS decimal,
          1.23::float4 AS float,
          1.23::float8 AS double,
          'text'::varchar AS varchar,
          '2024-01-01'::date AS date,
          '2024-01-01 12:00:00'::timestamptz AS timestamp,
          E'\\xDEADBEEF'::bytea AS bytea
      """)
      stmt.getQueryId()
    }.get

    val row = spark.read
      .format("com.salesforce.datacloud.spark.HyperResultSource")
      .option("port", hyperServerProcess.getPort())
      .option("query_id", queryId)
      .load()
      .head()

    // Verify the schema of the result.
    val expectedFields = Array(
      StructField("boolean", BooleanType, nullable = false),
      StructField("smallint", ShortType, nullable = false),
      StructField("int", IntegerType, nullable = false),
      StructField("bigint", LongType, nullable = false),
      StructField("decimal", DecimalType(10, 2), nullable = false),
      StructField("float", FloatType, nullable = false),
      StructField("double", DoubleType, nullable = false),
      StructField("varchar", StringType, nullable = false),
      StructField("date", DateType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("bytea", BinaryType, nullable = false)
    )
    val actualFields = row.schema.fields
    assert(
      actualFields.length == expectedFields.length,
      s"Expected ${expectedFields.length} fields but got ${actualFields.length}"
    )
    for (i <- expectedFields.indices) {
      val expected = expectedFields(i)
      val actual = actualFields(i)
      assert(
        expected.name == actual.name,
        s"Field $i: expected name '${expected.name}' but got '${actual.name}'"
      )
      assert(
        expected.dataType == actual.dataType,
        s"Field $i '${expected.name}': expected type ${expected.dataType} but got ${actual.dataType}"
      )
      assert(
        expected.nullable == actual.nullable,
        s"Field $i '${expected.name}': expected nullable=${expected.nullable} but got ${actual.nullable}"
      )
    }

    // Verify the values of the result.
    assert(row.getAs[Boolean]("boolean") == true)
    assert(row.getAs[Short]("smallint") == 1)
    assert(row.getAs[Int]("int") == 2)
    assert(row.getAs[Long]("bigint") == 3)
    assert(row.getAs[BigDecimal]("decimal") == new BigDecimal("1.23"))
    assert(row.getAs[Float]("float") == 1.23f)
    assert(row.getAs[Double]("double") == 1.23)
    assert(row.getAs[String]("varchar") == "text")
    assert(row.getAs[Date]("date") == Date.valueOf("2024-01-01"))
    assert(
      row.getAs[Timestamp]("timestamp") == Timestamp.valueOf(
        "2024-01-01 12:00:00"
      )
    )
    assert(
      row
        .getAs[Array[Byte]]("bytea")
        .sameElements(Array(0xde.toByte, 0xad.toByte, 0xbe.toByte, 0xef.toByte))
    )
  }

  test("supports reading multiple chunks and reports metrics") {
    val queryId = Using.Manager { use =>
      val connection = use(hyperServerProcess.getConnection());
      val stmt =
        use(connection.createStatement().unwrap(classOf[DataCloudStatement]))
      stmt.execute("SELECT generate_series(1, 1000) AS id")
      stmt.getQueryId()
    }.get

    val rows = spark.read
      .format("com.salesforce.datacloud.spark.HyperResultSource")
      .option("port", hyperServerProcess.getPort())
      .option("query_id", queryId)
      .load()
      .collect()

    // Verify that we read all rows.
    assert(rows.length == 1000)

    // Retrieve the metrics from the status store.
    val statusStore = spark.sharedState.statusStore
    val execId = statusStore.executionsList.last.executionId
    val chunkCountMetricId =
      statusStore
        .execution(execId)
        .get
        .metrics
        .find(_.name == "number of chunks in the result set from Hyper")
        .get

    val metricValues = statusStore.executionMetrics(execId)
    val chunkCountMetric = metricValues(chunkCountMetricId.accumulatorId)
    assert(chunkCountMetric == "250")
  }
}
