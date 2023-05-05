import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import java.util.concurrent.TimeUnit

object StructuredStreamingConsumer {
  // spark上下文
  val spark = SparkSession.builder().appName("kafka_test").master("local[*]").enableHiveSupport().getOrCreate()
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

    val df1 = spark.table("src")
    df1.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")

    value.printSchema()
    value.writeStream
      .outputMode(OutputMode.Append())
      .format("orc")  //指定外部输出的文件存储格式
      .option("format", "append")
      .trigger(Trigger.ProcessingTime(10,TimeUnit.SECONDS))/**每60秒执行一次，不指定就是as fast as possible*/
      .option("checkpointLocation","hdfs:/xxxxx:8020/tmp/offset/test/StructuredStreamingFromKafka2Hive") /**用来保存offset，用该目录来绑定对应的offset，如果该目录发生改变则程序运行的id会发生变化，类比group.id的变化，写hive的时候一定不要轻易变动*/
      .partitionBy("year","month","day")//提供分区字段




//    value.writeStream
//      .outputMode(OutputMode.Append())
//      .format("console")
//      .start()

  }
}
