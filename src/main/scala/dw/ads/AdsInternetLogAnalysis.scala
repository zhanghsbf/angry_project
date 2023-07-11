package dw.ads

import org.apache.spark.sql.execution.datasources.FileFormatWriter
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import dw.ads.models.LogInAnalysisModel
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.rdd.EsSpark

import java.util.concurrent.TimeUnit

object AdsInternetLogAnalysis {
  // spark上下文
  val spark = SparkSession.builder().appName("kafka_test").master("local[*]").enableHiveSupport().getOrCreate()

  import spark.implicits._
  FileFormatWriter

  def main(args: Array[String]): Unit = {
    val path:String = "/user/hive/warehouse/dw_ods.db/ods_kafka_internet_log"
    val query = sink2Es(doTransform(getHiveStream(path)))
    query.awaitTermination()
  }

  private def getHiveStream(path: String): DataFrame = {
    val df = spark.readStream.schema(
      """
        |client_ip string
        |,country string
        |,province string
        |,city string
        |,operator string
        |,domain string
        |,time string
        |,target_ip string
        |,rcode string
        |,query_type string
        |,authority_record string
        |,add_msg string
        |,dns_ip string
        |,year string
        |,month string
        |,day string
        |""".stripMargin).orc(path)
    df
  }

  private def doTransform(df: DataFrame): DataFrame = {
    df.printSchema()
    val value: DataFrame = df.map(row => {
      val clientIP = row.getAs[String]("client_ip")
      val country: String = row.getAs[String]("country")
      val province: String = row.getAs[String]("province")
      val city: String = row.getAs[String]("city")
      val year = row.getAs[String]("year")
      val month = row.getAs[String]("month")
      val day = row.getAs[String]("day")
      LogInAnalysisModel(clientIP, country, province, city, year, month, day)
    }).toDF()
    df.printSchema()
    value
  }



  def sink2Es(result: DataFrame): StreamingQuery = {
    val query = result.writeStream
      .trigger(Trigger.ProcessingTime(60, TimeUnit.SECONDS)) // 写入间隔
      .foreachBatch((ds, offset) => {
        //        ds.write
        //          .format("org.elasticsearch.spark.sql")
        //          .option("es.nodes", "http://zyk-bigdata-004:9200")
        //          .option("es.index.auto.create", "true")
        //          .option("es.resource", "login_analysis_model")
        //          .mode(SaveMode.Append)
        //          .save()
        val rdd: RDD[LogInAnalysisModel] = ds.rdd.map(row =>
          LogInAnalysisModel(row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6))
        )
        EsSpark.saveToEs(rdd, "login_analysis_model")
      })
      .option("checkpointLocation", "hdfs://zyk-bigdata-001:8020/tmp/offset/test/ads_log_in_analysis_model")
      .start()
    query
  }
}
