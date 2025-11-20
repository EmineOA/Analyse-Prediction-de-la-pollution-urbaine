import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

//VM options: --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
//-Dhadoop.home.dir=C:\\Users\\Administrateur\\IdeaProjects\\untitled1\\hadoop
//-Djava.library.path=C:\\Users\\Administrateur\\IdeaProjects\\untitled1\\hadoop\\bin
object ImportAirIDF {

  def main(args: Array[String]): Unit = {

    //SparkSession
    val spark = SparkSession.builder()
      .appName("Qualite_Air")
      .master("local[*]")
      .getOrCreate()

    //pour diminuer le bruit dans la console
    spark.sparkContext.setLogLevel("WARN")

    //Lecture du CSV
    val airPath: String = "H:\\\\Desktop\\\\2éme année" +
      "\\\\prog fonctionnelle" +
      "\\\\projet Spark"

    val airDFraw = spark.read
      .option("header", "true")
      .option("sep", ";")
      .option("interSchema", "true") //attribution ds types automatiques
      .csv(airPath)

    println("=== Structure du dataset QUALITE AIR IDF ===")
    airDFraw.printSchema()
    airDFraw.show(10, truncate = false)

    //Arrêt de Spark
    spark.stop()
  }
}
