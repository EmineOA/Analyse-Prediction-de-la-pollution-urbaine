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
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer") //Kyro pose des problèmes
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
    println("=== 20 premières lignes du dataset ===")
    df.show(20)

    // Filtrage avec filter
    import spark.implicits._
    println("=== Données avec \"stop_lon\" > 2.5 ===")
    val df_filtre = df.filter($"stop_lon" > 2.5)
    df_filtre.show(10)

    //Utilisation de la fonction map - stations les plus proches de Cergy Préfecture
    val cergyRow = df
      .filter($"Nom de la Station" === "Cergy Préfecture")
      .select("stop_lon", "stop_lat")
      .head()
    val cergyLon = cergyRow.getAs[Double]("stop_lon")
    val cergyLat = cergyRow.getAs[Double]("stop_lat")

    println(s"Cergy Préfecture : longitude = $cergyLon, latitude = $cergyLat")

    val rddStations = df
      .select(
        $"Nom de la Station".as("station"),
        $"Nom de la ligne".as("ligne"),
        $"stop_lon",
        $"stop_lat"
      )
      .na.drop(Seq("stop_lon", "stop_lat")) //on enlève les stations sans coordonnées
      .rdd

    //utilisation de la fonction map
    val distancesRDD = rddStations.map { row =>
      val station = row.getAs[String]("station")
      val ligne   = row.getAs[String]("ligne")
      val lon     = row.getAs[Double]("stop_lon")
      val lat     = row.getAs[Double]("stop_lat")

      val dx = lon - cergyLon
      val dy = lat - cergyLat
      val dist = math.sqrt(dx*dx + dy*dy)

      (dist, station, ligne, lon, lat)
    }

    val distancesSortedRDD =
      distancesRDD.sortBy(_._1, ascending = true)

    val distancesDF = distancesSortedRDD.toDF(
      "distance",
      "station",
      "ligne",
      "stop_lon",
      "stop_lat"
    )

    println("=== Stations les plus proches de Cergy Préfecture ===")
    distancesDF.show(20, truncate = false)

    //Utilisation de flatmap - récupération de la liste des lignes
    val ds = df
      .select($"Nom de la ligne".as[String])
      .as[String]

    val lignesFlat = ds.flatMap(line => Option(line))
      .distinct()

    lignesFlat.show(200, truncate = false)

    //Moyenne, maximum et minimum de la latitude et longitude
    print("=== Moyenne, maximum, minimum ===")
    df.agg(avg("stop_lon"), min("stop_lon"), max("stop_lon")).show()
    df.agg(avg("stop_lat"), min("stop_lat"), max("stop_lat")).show()

    //Arrêt de Spark
    spark.stop()
  }

}