package DDSpark

import DDSpark.Tasks.{runPartA, runPartB, runPartC}
import DDSpark.Utils.schemaTransactions
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class TestTasksABC
    extends FunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    // Create a SparkSession
    spark = SparkSession
      .builder()
      .appName("TestPartA")
      .config("spark.executor.memory", "10g")
      .config("spark.master", "local[*]")
      .config("spark.driver.memory", "4g")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.sparkContext.stop()
  }

  test("Part A should return the correct counts and only count distinct rows") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("user1", "1", "a1", "aName1", "t1", "tName1"),
          Row("user1", "2", "a2", "aName2", "t2", "tName3"),
          Row("user1", "3", "a2", "aName2", "t2", "tName3"),
          Row("user2", "3", "a3", "aName3", "t3", "tName2")
        )
      ),
      schemaTransactions
    )
    val output = runPartA(df)(spark)
    assert(output.count() === 2)
    assert(
      output.filter(_.get(0) == "user1").first().getLong(1) === 2)
    assert(
      output.filter(_.get(0) == "user2").first().getLong(1) === 1)
  }

  test(
    "Part B should be ordered, contain two rows, be " +
      "sorted in desc order and ignore null value rows") {
    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("user1", "1", "a1", "aName1", "t1", "tName1"),
          Row("user4", "1", "a1", "aName1", "t1", "tName1"),
          Row("user5", "1", "a1", "aName1", "t1", "tName1"),
          Row("user2", "3", "a3", "aName3", "t3", "tName2"),
          Row("user3", "3", "a3", null, "t3", "tName2"),
          Row("user3", "3", "a3", "aName3", "t3", null)
        )
      ),
      schemaTransactions
    )
    val output2 = runPartB(df2)(spark)
    assert(output2.count() === 2)
    assert(output2.first().getLong(2) === 3)
    assert(output2.filter(_.get(0) == "aName3").first().getLong(2) === 1)
  }

  test(
    "Part C") {
    val dfc = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("user1", "2017-06-19T19:01:01Z", "a1", "aName1", "t1", "tName1"),
          Row("user1", "2017-06-19T19:02:01Z", "a1", "aName2", "t1", "tName2"),

          Row("user2", "2017-06-19T19:01:01Z", "a1", "aName1", "t1", "tName1"),
          Row("user2", "2017-06-19T19:03:01Z", "a1", "aName2", "t1", "tName2"),

          Row("user3", "2017-06-19T19:01:01Z", "a1", "aName1", "t1", "tName1"),
          Row("user3", "2017-06-19T19:03:01Z", "a1", "aName2", "t1", "tName2"),

          Row("user4", "2017-06-19T19:01:01Z", "a1", "aName1", "t1", "tName1"),
          Row("user4", "2017-06-19T19:03:01Z", "a1", "aName2", "t1", "tName2"),

          Row("user5", "2017-06-19T19:01:01Z", "a1", "aName1", "t1", "tName1"),
          Row("user5", "2017-06-19T19:03:01Z", "a1", "aName2", "t1", "tName2"),

          Row("user6", "2017-06-19T19:01:01Z", "a1", "aName1", "t1", "tName1"),
          Row("user6", "2017-06-19T19:03:01Z", "a1", "aName2", "t1", "tName2"),

          Row("user7", "2017-06-19T19:01:01Z", "a1", "aName1", "t1", "tName1"),
          Row("user7", "2017-06-19T19:03:01Z", "a1", "aName2", "t1", "tName2"),

          Row("user8", "2017-06-19T19:01:01Z", "a1", "aName1", "t1", "tName1"),
          Row("user8", "2017-06-19T19:03:01Z", "a1", "aName2", "t1", "tName2"),

          Row("user9", "2017-06-19T19:01:01Z", "a1", "aName1", "t1", "tName1"),
          Row("user9", "2017-06-19T19:03:01Z", "a1", "aName2", "t1", "tName2"),

          Row("user10", "2017-06-19T19:01:01Z", "a1", "aName1", "t1", "tName1"),
          Row("user10", "2017-06-19T19:03:01Z", "a1", "aName2", "t1", "tName2"),

          Row("user11", "2017-06-19T19:01:01Z", "a1", "aName1", "t1", "tName1"),
          Row("user11", "2017-06-19T19:19:01Z", "a1", "aName2", "t1", "tName2")
        )),
      schemaTransactions
    )
    val output3 = runPartC(20, dfc)(spark)
    assert(output3.count() === 20)
    assert(output3.filter(_.get(0) == "user1").count() === 0)
    assert(output3.agg(max("sessionDuration")).head.getLong(0) == 1080)
  }

}
