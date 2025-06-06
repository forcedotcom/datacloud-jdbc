import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.scalatest.Suite
import com.salesforce.datacloud.jdbc.hyper.HyperServerProcess

trait WithSparkSession extends BeforeAndAfterAll { self: Suite =>
  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("datacloud-jdbc-test")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    // Set root logger to ERROR to suppress most logs
    Logger.getRootLogger.setLevel(Level.ERROR)
    // Set Spark's log level to ERROR to suppress noise
    // It's not sufficient to only set the root logger
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    super.afterAll();
    spark.stop()
  }
}

trait WithHyperServer extends BeforeAndAfterAll { self: Suite =>
  var hyperServerProcess: HyperServerProcess = _;

  override def beforeAll(): Unit = {
    super.beforeAll()
    this.hyperServerProcess = new HyperServerProcess()
  }

  override def afterAll(): Unit = {
    super.afterAll();
    if (this.hyperServerProcess != null) {
      this.hyperServerProcess.close()
    }
  }
}
