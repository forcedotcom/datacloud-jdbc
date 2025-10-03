import org.scalatest.funsuite.AnyFunSuite
import scala.util.Using
import com.salesforce.datacloud.jdbc.core.DataCloudStatement
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperServerManager
import com.salesforce.datacloud.jdbc.hyper.HyperServerManager.ConfigFile
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

object HyperResultSourceTest {
  Class.forName("com.salesforce.datacloud.jdbc.HyperJDBCDriver");
}

class HyperResultSourceTest extends AnyFunSuite with WithSparkSession {
  val hyperServerProcess = HyperServerManager.get(ConfigFile.SMALL_CHUNKS);

  test("can connect using a jdbc url") {
    val queryId = Using.Manager { use =>
      val connection = use(hyperServerProcess.getConnection());
      val stmt =
        use(connection.createStatement().unwrap(classOf[DataCloudStatement]))
      stmt.execute("SELECT 42::bigint AS bigint")
      stmt.getQueryId()
    }.get

    val row = spark.read
      .format("com.salesforce.datacloud.spark.HyperResultSource")
      .option(
        "jdbcUrl",
        s"jdbc:salesforce-hyper://localhost:${hyperServerProcess.getPort()}"
      )
      .option("queryId", queryId)
      .load()
      .head()

    // Verify the schema of the result.
    assert(row.getAs[Long]("bigint") == 42)
  }

  test("reports an error on missing query id") {
    val e = intercept[IllegalArgumentException] {
      spark.read
        .format("com.salesforce.datacloud.spark.HyperResultSource")
        .option(
          "jdbcUrl",
          s"jdbc:salesforce-hyper://localhost:${hyperServerProcess.getPort()}"
        )
        .load()
    }
    assert(e.getMessage.equals("Missing `queryId` property"))
  }

  test("reports an error on invalid query id") {
    val e = intercept[DataCloudJDBCException] {
      spark.read
        .format("com.salesforce.datacloud.spark.HyperResultSource")
        .option(
          "jdbcUrl",
          s"jdbc:salesforce-hyper://localhost:${hyperServerProcess.getPort()}"
        )
        .option("queryId", "invalid")
        .load()
    }
    assert(
      e.getMessage().contains("The requested query ID is unknown"),
      e.getMessage()
    )
  }

  test("reports an error on unknown properties") {
    val e = intercept[DataCloudJDBCException] {
      spark.read
        .format("com.salesforce.datacloud.spark.HyperResultSource")
        .option(
          "jdbcUrl",
          s"jdbc:salesforce-hyper://localhost:${hyperServerProcess.getPort()}"
        )
        .option("queryId", "143241")
        .option("unknownProperty", "maybe")
        .load()
    }
    assert(
      e.getMessage().equals("Unknown JDBC properties: unknownProperty"),
      e.getMessage()
    )
  }

  test("supports reading all types") {
    val queryId = Using.Manager { use =>
      val connection = use(hyperServerProcess.getConnection());
      val stmt =
        use(connection.createStatement().unwrap(classOf[DataCloudStatement]))
      stmt.execute("""
        SELECT
          true::boolean AS boolean,
          NULL::boolean AS boolean_null,
          1::smallint AS smallint,
          NULL::smallint AS smallint_null,
          2::int AS int,
          NULL::int AS int_null,
          3::bigint AS bigint,
          NULL::bigint AS bigint_null,
          1.23::decimal(3,2) AS decimal_3_2,
          1.2345::decimal(10,4) AS decimal_10_4,
          NULL::decimal(10,2) AS decimal_null,
          1.23::float4 AS float,
          NULL::float4 AS float_null,
          1.23::float8 AS double,
          NULL::float8 AS double_null,
          'text'::varchar AS varchar,
          NULL::varchar AS varchar_null,
          '2024-01-01'::date AS date,
          NULL::date AS date_null,
          '2024-01-01 12:00:00'::timestamptz AS timestamp,
          NULL::timestamptz AS timestamp_null,
          E'\\xDEADBEEF'::bytea AS bytea,
          NULL::bytea AS bytea_null
      """)
      stmt.getQueryId()
    }.get

    val row = spark.read
      .format("com.salesforce.datacloud.spark.HyperResultSource")
      .option(
        "jdbcUrl",
        s"jdbc:salesforce-hyper://localhost:${hyperServerProcess.getPort()}"
      )
      .option("queryId", queryId)
      .load()
      .head()

    // Verify the schema of the result.
    val expectedFields = Array(
      StructField("boolean", BooleanType, nullable = false),
      StructField("boolean_null", BooleanType, nullable = true),
      StructField("smallint", ShortType, nullable = false),
      StructField("smallint_null", ShortType, nullable = true),
      StructField("int", IntegerType, nullable = false),
      StructField("int_null", IntegerType, nullable = true),
      StructField("bigint", LongType, nullable = false),
      StructField("bigint_null", LongType, nullable = true),
      StructField("decimal_3_2", DecimalType(3, 2), nullable = false),
      StructField("decimal_10_4", DecimalType(10, 4), nullable = false),
      StructField("decimal_null", DecimalType(10, 2), nullable = true),
      StructField("float", FloatType, nullable = false),
      StructField("float_null", FloatType, nullable = true),
      StructField("double", DoubleType, nullable = false),
      StructField("double_null", DoubleType, nullable = true),
      StructField("varchar", StringType, nullable = false),
      StructField("varchar_null", StringType, nullable = true),
      StructField("date", DateType, nullable = false),
      StructField("date_null", DateType, nullable = true),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("timestamp_null", TimestampType, nullable = true),
      StructField("bytea", BinaryType, nullable = false),
      StructField("bytea_null", BinaryType, nullable = true)
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
    assert(row.isNullAt(row.fieldIndex("boolean_null")))
    assert(row.getAs[Short]("smallint") == 1)
    assert(row.isNullAt(row.fieldIndex("smallint_null")))
    assert(row.getAs[Int]("int") == 2)
    assert(row.isNullAt(row.fieldIndex("int_null")))
    assert(row.getAs[Long]("bigint") == 3)
    assert(row.isNullAt(row.fieldIndex("bigint_null")))
    assert(row.getAs[BigDecimal]("decimal_3_2") == new BigDecimal("1.23"))
    assert(row.getAs[BigDecimal]("decimal_10_4") == new BigDecimal("1.2345"))
    assert(row.isNullAt(row.fieldIndex("decimal_null")))
    assert(row.getAs[Float]("float") == 1.23f)
    assert(row.isNullAt(row.fieldIndex("float_null")))
    assert(row.getAs[Double]("double") == 1.23)
    assert(row.isNullAt(row.fieldIndex("double_null")))
    assert(row.getAs[String]("varchar") == "text")
    assert(row.isNullAt(row.fieldIndex("varchar_null")))
    assert(row.getAs[Date]("date") == Date.valueOf("2024-01-01"))
    assert(row.isNullAt(row.fieldIndex("date_null")))
    assert(
      row.getAs[Timestamp]("timestamp") == Timestamp.valueOf(
        "2024-01-01 12:00:00"
      )
    )
    assert(
      row.isNullAt(row.fieldIndex("timestamp_null"))
    )
    assert(
      row
        .getAs[Array[Byte]]("bytea")
        .sameElements(Array(0xde.toByte, 0xad.toByte, 0xbe.toByte, 0xef.toByte))
    )
    assert(
      row.isNullAt(row.fieldIndex("bytea_null"))
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
      .option(
        "jdbcUrl",
        s"jdbc:salesforce-hyper://localhost:${hyperServerProcess.getPort()}"
      )
      .option("queryId", queryId)
      .load()
      .collect()

    // Verify that we read all rows.
    assert(rows.length == 1000)
    assert(rows.map(_.getAs[Int]("id")).toSet == (1 to 1000).toSet)

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

  test("supports empty result sets with basic schema and nullable columns") {
    val queryId = Using.Manager { use =>
      val connection = use(hyperServerProcess.getConnection());
      val stmt =
        use(connection.createStatement().unwrap(classOf[DataCloudStatement]))
      stmt.execute("""
        SELECT
          1::int AS id,
          NULL::varchar AS name,
          NULL::decimal(10,2) AS amount,
          NULL::timestamp AS created_at,
          NULL::boolean AS is_active
        WHERE 1 = 0
      """)
      stmt.getQueryId()
    }.get

    val df = spark.read
      .format("com.salesforce.datacloud.spark.HyperResultSource")
      .option(
        "jdbcUrl",
        s"jdbc:salesforce-hyper://localhost:${hyperServerProcess.getPort()}"
      )
      .option("queryId", queryId)
      .load()

    // Verify schema is correctly inferred with nullable columns
    assert(df.schema.fields.length == 5)

    val fields = df.schema.fields
    assert(fields(0).name == "id")
    assert(fields(0).dataType == IntegerType && !fields(0).nullable)
    assert(fields(1).name == "name")
    assert(fields(1).dataType == StringType && fields(1).nullable)
    assert(fields(2).name == "amount")
    assert(fields(2).dataType == DecimalType(10, 2) && fields(2).nullable)
    assert(fields(3).name == "created_at")
    assert(fields(3).dataType == TimestampType && fields(3).nullable)
    assert(fields(4).name == "is_active")
    assert(fields(4).dataType == BooleanType && fields(4).nullable)

    // Verify no rows are returned
    assert(df.count() == 0)

    // Verify we can still perform operations on the empty DataFrame
    val filtered = df.filter(df("amount") > 50.0)
    assert(filtered.count() == 0)
  }
}
