import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestSparkIntelliJ")
      .master("local[*]") // utilise tous les cœurs disponibles
      .getOrCreate()

    import spark.implicits._

    val df = Seq(1, 2, 3, 4, 5).toDF("valeur")
    df.show()

    spark.stop()
  }
}
