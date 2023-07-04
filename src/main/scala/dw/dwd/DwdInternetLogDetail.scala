package dw.dwd

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.concurrent.TimeUnit

object DwdInternetLogDetail {
  // spark上下文
  val spark = SparkSession.builder().appName("kafka_test").master("local[*]").enableHiveSupport().getOrCreate()

  import spark.implicits._


  def main(args: Array[String]): Unit = {
    val topic: String = "realtime_data"
    val query = sink2Hive(doTransform(getSource(topic)))
    query.awaitTermination()
  }

  private def getHiveStream(path: String): DataFrame = {
    val df = spark.readStream.schema("id String,domain String,time String,target_ip String,rcode String,query_type String,authority_record String,add_msg String,dns_ip String,year String,month String,day String")
                    .orc("/user/hive/warehouse/zyktest.db/test")
    df
  }

  private def doTransform(df: DataFrame): DataFrame = {
    //读kafka取出 message并做基本的清洗，默认值，添加上年月日字段
    df.printSchema()
    val value: DataFrame = df.selectExpr("cast(value as STRING)").map(row => {
      val kafkaStr = row.getString(0)
      val message = JSON.parseObject(kafkaStr).getString("message")
      val msgArray = message.split(",") //指定分隔符进行字段切分
      msgArray
    }).filter(_.length == 9) //只留字段数为9的数据
      .filter(array => array(2).length >= 8) //确保日期字段符合规范
      .map(array => (array(0) + array(1) + array(2)
        , array(0)
        , array(1)
        , array(2)
        , array(3)
        , array(4)
        , array(5)
        , array(6)
        , array(7)
        , array(8)
        , array(2).substring(0, 4)
        , array(2).substring(4, 6)
        , array(2).substring(6, 8)
      ))
      .toDF("id", "client_ip", "domain", "time", "target_ip", "rcode", "query_type", "authority_record", "add_msg", "dns_ip", "year", "month", "day")

    value
  }

  def sink2Hive(result: DataFrame): StreamingQuery = {
    val query = result.writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(60, TimeUnit.SECONDS)) // 写入间隔
      .foreachBatch((ds, offset) => {
        ds.write
          .format("orc")
          .mode(SaveMode.Append)
          .partitionBy("year", "month", "day")
          .saveAsTable("dw_ods.ods_kafka_internet_log")
      })
      .option("checkpointLocation", "hdfs://zyk-bigdata-001:8020/tmp/offset/test/ods_kafka_internet_log")
      .start()
    query
  }
}
