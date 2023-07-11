import org.apache.spark.sql.{DataFrame, SparkSession}
import dw.ads.models.LogInAnalysisModel
object SparkTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("kafka_test").master("local[*]").getOrCreate()

    import spark.implicits._

    val df = spark.createDataset(Seq("111.22.11.11", "asd", "asd", "asd", "asd", "asd", "asd"))
      .toDF("client_ip String,country String, province String, city String,year String,month String,day String")
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
    df.show
  }
}
