import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object StructuredStreamingConsumer {
  // spark上下文
  val spark = SparkSession.builder().appName("kafka_test").master("local[*]").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","43.139.144.33:9092")
      .option("subscribe","quickstart-events")
      .load()

    val value: Dataset[String] = df.selectExpr("cast(value as STRING)").map(row => {
      row.getString(0)
    })

    value.printSchema()

    value.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .start()
  }
}
