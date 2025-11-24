import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

//VM options: --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
//-Dhadoop.home.dir=C:\\Users\\Administrateur\\IdeaProjects\\untitled1\\hadoop
//-Djava.library.path=C:\\Users\\Administrateur\\IdeaProjects\\untitled1\\hadoop\\bin

object ImportAirIDF {

  def main(args: Array[String]): Unit = {

    /*
      1) Ingestion et préparation des données
        - lecture des fichiers CSV ou JSON en Spark;
        - suppression des doublons et des valeurs manquantes
     */

    //Ouverture d'une session spark
    val spark = SparkSession.builder()
      .appName("Qualite_Air")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer") //Kyro pose des problèmes
      .getOrCreate()

    //Imports complémentaires
    import spark.implicits._

    //Diminution du bruit dans la console
    spark.sparkContext.setLogLevel("WARN")

    //Types des attributs
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

    //Lecture des datasets
    val df = spark.read
      .format("csv")
      .option("sep", ";")
      .option("header", true)
      .schema(schema)
      .load("./data/idf_data.csv")

    val pm10_raw = spark.read
      .format("csv")
      .option("header", false)
      .option("sep", ",")
      .load("./data/pm10.csv")

    val pm25_raw = spark.read
      .format("csv")
      .option("header", false)
      .option("sep", ",")
      .load("./data/pm25.csv")

    println("Nombres de lignes avant nettoyage : " + df.count())

    //Suppression des doublons et des valeurs manquantes
    val df_cleaned = df.na.drop().dropDuplicates()
    val pm10_cleaned = pm10_raw.drop().dropDuplicates()
    val pm25_cleaned = pm25_raw.drop().dropDuplicates()

    //println("Nombres de lignes après nettoyage : " + df.count())

    //Affichage
    println("=== 5 premières lignes des dataset ===")
    df.show(5)
    pm10_raw.show(5)
    pm25_raw.show(5)

    /* 1-bis) Restructuration des data PM10 et PM25*/


    /*
    2) Transformation et exploration fonctionnelle
      - Utilisation de map, filter et flatmap pour transformer les données;
      - Calcul de statistiques par station ou par ligne (moyenne, maximum, minimum);
      - Extraction de variables temporelles pertinentes (heure, jour, mois);
     */

    //Filter - Filtrage selon la longitude
    println("=== Données avec \"stop_lon\" > 2.5 ===")
    val df_filtre = df.filter($"stop_lon" > 2.5)
    df_filtre.show(10)

    //Map - Stations les plus proches de Cergy Préfecture
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

    //FlatMap - récupération du nom des lignes en IDF
    val ds = df
      .select($"Nom de la ligne".as[String])
      .as[String]

    val lignesFlat = ds.flatMap(line => Option(line))
      .distinct()

    lignesFlat.show(200, truncate = false)

    //Statistiques descriptives : Moyenne, maximum et minimum (latitude et longitude)
    print("=== Moyenne, maximum, minimum ===")
    df.agg(avg("stop_lon"), min("stop_lon"), max("stop_lon")).show()
    df.agg(avg("stop_lat"), min("stop_lat"), max("stop_lat")).show()


    /*
      3) Analyse approfondie
        - Identification des stations les plus exposées à la pollution
        - Détection des pics horaires et périodes critiques
        - Création d'un indicateur global de pollution
        - Détection automatique des anomalies dans les données
     */

    //Variable qualitative pollution -> Variable quantitative
    val dfScored = df.withColumn(
      "score_pollution",
      when($"niveau_pollution" === "pollution faible",   lit(1))
        .when($"niveau_pollution" === "pollution moyenne", lit(2))
        .when($"niveau_pollution" === "pollution forte",   lit(3))
        .when($"niveau_pollution" === "station aérienne",  lit(0)) // ou null si tu préfères
        .otherwise(lit(null).cast("int"))
    )

    //Identification des stations les plus exposées les plus pollués
    val stationsExpo = dfScored
      .groupBy("Nom de la Station", "Nom de la ligne")
      .agg(
        avg("score_pollution").as("score_moyen"),
        max("score_pollution").as("score_max")
      )
      .orderBy(desc("score_moyen"), desc("score_max"))

    println("=== Stations les plus exposées ===")
    stationsExpo.show(20, truncate = false)

    //Détection des pics horaires et périodes critiques (utilise un dataset complémentaire)


    //Création d'un indicateur global de pollution
    val global = dfScored
      .agg(avg("score_pollution").as("score_moyen"))
      .withColumn("indice_global_0_100", $"score_moyen" / 3 * 100)  // 3 = score max théorique

    println("=== Indicateur global de pollution ===")
    global.show(false)

    //Détection automatique des anomalies dans les données
    //Stations "outliers" par rapport à leur ligne
    val statsLigne = dfScored
      .groupBy("Nom de la ligne")
      .agg(
        avg("score_pollution").as("moy_ligne"),
        stddev("score_pollution").as("sd_ligne")
      )

    val dfZ = dfScored
      .join(statsLigne, "Nom de la ligne")
      .withColumn("z_score",
        ($"score_pollution" - $"moy_ligne") / $"sd_ligne"
      )

    val anomaliesStations = dfZ.filter($"z_score" > 1.5) // seuil à ajuster

    println("=== Stations très anormales pour leur ligne ===")
    anomaliesStations.select(
        "Nom de la Station",
        "Nom de la ligne",
        "score_pollution",
        "moy_ligne",
        "sd_ligne",
        "z_score"
      ).orderBy(desc("z_score"))
      .show(50, false)

    //Arrêt de Spark
    spark.stop()
  }

}