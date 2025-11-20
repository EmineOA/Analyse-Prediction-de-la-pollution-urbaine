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

    //Précision sur les types
    import org.apache.spark.sql.types._

    val schema = StructType(Seq(
      StructField("Identifiant station", StringType, true),
      StructField("Nom de la Station", StringType, true),
      StructField("Nom de la ligne", StringType, true),
      StructField("Niveau de pollution aux particules", StringType, true),
      StructField("Niveau de pollution", StringType, true),
      StructField("Incertitude", StringType, true),
      StructField("Recommandation de surveillance", StringType, true),
      StructField("Action(s) QAI en cours", StringType, true),
      StructField("Lien vers les mesures en direct", StringType, true),
      StructField("Durée des mesures", StringType, true),
      StructField("Mesures d’amélioration mises en place ou prévues", StringType, true),
      StructField("stop_lon", DoubleType, true),
      StructField("stop_lat", DoubleType, true),
      StructField("point_geo", StringType, true),
      StructField("pollution_air", StringType, true),
      StructField("air", StringType, true),
      StructField("niveau", StringType, true),
      StructField("actions", StringType, true),
      StructField("niveau_pollution", StringType, true)
    ))

    //Lecture du CSV
    val df = spark.read
      .format("csv")
      .option("sep", ";")
      .option("header", true)
      .schema(schema)
      .load("./data/idf_data.csv")
    println("=== Structure du dataset QUALITE AIR IDF ===")

    println("Nombres de lignes avant nettoyage : " + df.count())
    //Suppression des doublons et des valeurs manquantes
    val df_cleaned = df.na.drop().dropDuplicates()
    println("Nombres de lignes après nettoyage : " + df.count())

    //Affichage
    println("=== Données nettoyées ===")
    df.show(20)

    // Requête SQL
    println("=== Données avec \"stop_lon\" > 2.5 ===")
    df.createOrReplaceTempView("pollution")
		var df_req = spark.sql("SELECT * FROM pollution WHERE stop_lon > 2.5")
		df_req.show()
		
		println("Nombres de lignes retenues après la requête : " + df_req.count())

    //Arrêt de Spark
    spark.stop()
  }
}
