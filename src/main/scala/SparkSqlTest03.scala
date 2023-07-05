import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window


/**
 * @DESC: 连续登陆领金币问题
 * @Auther: Anryg
 * @Date: 2022/09/13 12:10
 */
object SparkSQLTest03 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("连续登陆领金币问题")
    sparkConf.setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
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

    val df = spark.createDataset(seq).toDF("uid","article_id","in_time","out_time","if_sign")
    df.show()
    df.printSchema()
    import org.apache.spark.sql.functions._  /**引入spark内置函数*/

    val df1 = df.select($"uid",$"in_time")
      .where(to_date($"in_time").between("2021-07-07 00:00:00","2021-10-31 23:59:59"))
      .where($"article_id" === 0 and $"if_sign" === 1 )
    df1.show()

    val df2 = df1.withColumn("row_number", row_number() over(Window.partitionBy($"uid").orderBy($"uid",$"in_time")))
    df2.show()
    df2.printSchema()

    val df3 = df2.selectExpr("uid", "in_time", "row_number", "date_sub(in_time, row_number) as sub_time")
//      withColumn("sub_time", date_sub($"in_time", $"row_number"))
    df3.show()

    val df4 = df3.select($"uid", date_format($"in_time","yyyy-MM").as("month"), $"sub_time")
    df4.show()

    val df5 = df4.withColumn("continuous_login", row_number().over(Window.partitionBy($"uid",$"month",$"sub_time").orderBy($"uid",$"month",$"sub_time")))
    df5.show()

    val df6 = df5.withColumn("coin_num",
      when($"continuous_login"%7 === 0,7)
        .when($"continuous_login"%7 === 3, 3)
        .otherwise(1))
    df6.show()

    val result = df6.select($"uid", $"month", $"coin_num").groupBy($"uid", $"month").agg(sum($"coin_num").as("sum_coin"))
    result.show()

  }
}