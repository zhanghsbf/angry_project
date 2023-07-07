import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DemoFromKafka2Hive {
  // spark上下文
  val spark = SparkSession.builder().appName("连续登陆领金币问题").master("local[*]").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val seq = Seq(
      (101, 0, "2021-07-07 10:00:00", "2021-07-07 10:00:09", 1),
      (101, 0, "2021-07-08 10:00:00", "2021-07-08 10:00:09", 1),
      (101, 0, "2021-07-09 10:00:00", "2021-07-09 10:00:42", 1),
      (101, 0, "2021-07-10 10:00:00", "2021-07-10 10:00:09", 1),
      (101, 0, "2021-07-12 10:00:28", "2021-07-12 10:00:50", 1),
      (101, 0, "2021-07-13 10:00:32", "2021-07-13 10:00:50", 1),
      (101, 0, "2021-07-17 09:00:09", "2021-07-13 10:00:50", 1),
      (101, 0, "2021-07-18 09:30:09", "2021-07-13 10:00:50", 1),
      (101, 0, "2021-07-19 09:45:09", "2021-07-13 10:00:50", 1),
      (101, 0, "2021-07-20 09:58:09", "2021-07-13 10:00:50", 1),
      (101, 0, "2021-07-15 10:00:28", "2021-07-13 10:00:50", 1),
      (101, 0, "2021-07-16 11:00:28", "2021-07-13 10:00:50", 1),
      (101, 0, "2021-07-14 10:00:18", "2021-07-13 10:00:50", 1),
      (102, 0, "2021-10-01 10:00:20", "2021-10-01 10:00:50", 1),
      (101, 0, "2021-07-11 23:59:55", "2021-07-11 23:59:59", 1),
      (102, 0, "2021-10-02 10:00:01", "2021-10-02 10:01:50", 1),
      (102, 0, "2021-10-03 11:00:55", "2021-10-03 11:00:59", 1),
      (102, 0, "2021-10-04 11:00:45", "2021-10-04 11:00:55", 0),
      (102, 0, "2021-10-05 11:00:53", "2021-10-05 11:00:59", 1),
      (102, 0, "2021-10-06 11:00:45", "2021-10-06 11:00:55", 1)
    )
    val df: DataFrame = spark.createDataset(seq).toDF("uid","article_id","in_time","out_time","if_sign")
    val baseData = df.where($"article_id" === 0 and $"if_sign" === 1 and $"in_time".between("2021-07-07 00:00:00","2021-10-31 23:59:59" ))

    val rnk = baseData.withColumn("rn", row_number() over Window.partitionBy($"uid").orderBy($"in_time"))
    val result = rnk.selectExpr("uid", "in_time", "date_sub(to_date(in_time), rn) as sub_dt")
      .withColumn("continuous_login", row_number() over Window.partitionBy("uid", "sub_dt").orderBy("in_time"))
      .selectExpr("uid", "substr(to_date(in_time),1,7) as month", "sub_dt"
                , "case when continuous_login % 7 = 0 then 6" +
                  "     when continuous_login % 7 = 3 then 2" +
                  "     else 1" +
                  " end as gold ")
      .groupBy("uid","month").sum("gold")

//    rnk.selectExpr("uid", "to_date(in_time) as login_dt", "rn").withColumn("cnt", groupBy("uid","rn").count())

    result.printSchema()
    println(result.collect().mkString("\n"))
  }
}
