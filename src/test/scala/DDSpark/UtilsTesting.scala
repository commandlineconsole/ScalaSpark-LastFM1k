package DDSpark

import DDSpark.Utils.readTSV
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class UtilsTesting
    extends FunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    // Create a SparkSession
    spark = SparkSession
      .builder()
      .appName("UtilsTesting")
      .config("spark.executor.memory", "10g")
      .config("spark.master", "local[*]")
      .config("spark.driver.memory", "4g")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.sparkContext.stop()
  }

  test("Test reading a partial extract of the TSV file") {
    val input = readTSV(getClass.getResource("/headSeven.tsv").getPath, header = false)(spark)
    assert(input.count() === 7)
  }

  //  test("Test to convert strings to long format. Output is in ascending order.") {
  //    val df1 = spark.createDataFrame(
  //      spark.sparkContext.parallelize(
  //        Seq(
  //          Row("user1", "2017-06-19T19:01:01Z", "a1", "aName1", "t1", "tName1"),
  //          Row("user4", "2017-06-19T19:02:01Z", "a1", "aName1", "t1", "tName1")
  //        )
  //      ),
  //      schemaTransactions
  //    )
  //    val output2 = stringDate(df1, "TestDate", "timestamp")
  //    assert(output2.first().getLong(6) === 1497895261)
  //    assert(output2.first().getString(0) === "user1")
  //  }

}
