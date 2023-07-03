import org.apache.spark.sql.{Dataset, SparkSession}

object DemoFromKafka2Hive {
  // spark上下文
  val spark = SparkSession.builder().appName("kafka_test").master("local[*]").enableHiveSupport().getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {
      // 1. 接入kafka数据源
      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "zyk-bigdata-002:9092")
        .option("subscribe", "realtime_data")
        .load()
//
//    // kafka读出来默认是二进制，转为string
//    val value: Dataset[String] = df.selectExpr("cast(value as STRING)").map(row => {
//      row
//    })


  }
}
