package dw.dwd

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.execution.datasources.FileFormatWriter
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import utils.IPUtil

import java.util.concurrent.TimeUnit

object DwdInternetLogDetail {
  // spark上下文
  val spark = SparkSession.builder().appName("kafka_test").master("local[*]").enableHiveSupport().getOrCreate()

  import spark.implicits._
  FileFormatWriter

  def main(args: Array[String]): Unit = {
    val path:String = "/user/hive/warehouse/dw_ods.db/ods_kafka_internet_log"
    val query = sink2Hive(doTransform(getHiveStream(path)))
    query.awaitTermination()
  }

  private def getHiveStream(path: String): DataFrame = {
    val df = spark.readStream.schema("id String,client_ip String,domain String,time String,target_ip String,rcode String,query_type String,authority_record String,add_msg String,dns_ip String,year String,month String,day String")
                    .orc(path)
    df
  }

  private def doTransform(df: DataFrame): DataFrame = {
    df.printSchema()
    val value = df.map(row =>{
      val clientIP = row.getAs[String]("client_ip")
      val country:String = IPUtil.parseIpCountry(clientIP)      // 解析IP
      val province:String = IPUtil.parseIpProvince(clientIP)
      val city:String = IPUtil.parseIpCity(clientIP)
      val operator:String = null
      val domain = row.getAs[String]("domain").toLowerCase //将域名转成小写
      val time = row.getAs[String]("time")
      val targetIP = row.getAs[String]("target_ip")
      val rcode = row.getAs[String]("rcode")
      val queryType = row.getAs[String]("query_type")
      val authRecord = row.getAs[String]("authority_record").toLowerCase
      val addMsg = row.getAs[String]("add_msg")
      val dnsIP = row.getAs[String]("dns_ip")
      val year = row.getAs[String]("year")
      val month = row.getAs[String]("month")
      val day = row.getAs[String]("day")
      (clientIP, country, province, city, operator, domain, time, targetIP, rcode, queryType, authRecord, addMsg, dnsIP, year, month, day)
    }).toDF("client_ip","country","province","city","operator","domain","time","target_ip","rcode","query_type","authority_record","add_msg","dns_ip","year","month","day")
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
          .saveAsTable("dw_dwd.dwd_internet_log_detail")
      })
      .option("checkpointLocation", "hdfs://zyk-bigdata-001:8020/tmp/offset/test/dwd_kafka_internet_log")
      .start()
    query
  }
}
