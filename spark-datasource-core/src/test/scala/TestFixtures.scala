import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import org.scalatest.Suite
import com.salesforce.datacloud.jdbc.hyper.{
  HyperServerManager,
  HyperServerProcess
}
import com.salesforce.datacloud.jdbc.hyper.HyperServerManager.ConfigFile

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

    // Set Spark's log level to ERROR to suppress noise
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    super.afterAll();
    spark.stop()
  }
}
