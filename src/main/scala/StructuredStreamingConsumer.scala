import com.alibaba.fastjson.JSON
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.util
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt
import scala.util.parsing.json.JSONObject

object StructuredStreamingConsumer {
  // spark上下文
  val spark = SparkSession.builder().appName("kafka_test").master("local[*]").enableHiveSupport().getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","zyk-bigdata-002:9092")
      .option("subscribe","quickstart-events")
      .load()

//    读kafka取出 message并做基本的清洗，默认值，添加上年月日字段
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
        ,if (array(4).isInstanceOf[Int]) array(4).toInt else 99
        , array(5)
        , array(6)
        , array(7)
        , array(8)
        , array(2).substring(0, 4)
        , array(2).substring(4, 6)
        , array(2).substring(6, 8)
      ))
      .toDF("id","client_ip","domain","time","target_ip","rcode","query_type","authority_record","add_msg","dns_ip","year","month","day")

    value.writeStream
      .outputMode(OutputMode.Append())
      .format("parquet")
      .partitionBy("year","month","day")
      .trigger(Trigger.ProcessingTime(60,TimeUnit.SECONDS))  // 写入间隔
      .

  }
}
