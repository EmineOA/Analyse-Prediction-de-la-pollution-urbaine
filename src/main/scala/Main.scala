import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

//VM options: --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
//-Dhadoop.home.dir=C:\\Users\\Administrateur\\IdeaProjects\\untitled1\\hadoop
//-Djava.library.path=C:\\Users\\Administrateur\\IdeaProjects\\untitled1\\hadoop\\bin

object Main {

	def buildUniqueColNames(headerRow: Row, firstName: String = "datetime"): Array[String] = {
		val seen = scala.collection.mutable.Map[String, Int]()

		headerRow.toSeq.zipWithIndex.map {
			case (value, idx) =>
				if (idx == 0) {
					firstName
				}
				else {
					// valeur brute de la cellule d'en-tête
					val raw = Option(value).map(_.toString.trim).getOrElse("")
					val base =
						if (raw.isEmpty) s"col_$idx"
						else raw.replaceAll("[^A-Za-z0-9_-]", "_")	 // on nettoie un peu

					val count = seen.getOrElse(base, 0)
					seen.update(base, count + 1)

					if (count == 0) base			// première fois -> "RD934"
					else s"${base}_$count"			// deuxième fois -> "RD934_1", puis "RD934_2", etc.
				}
		}.toArray
	}

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

		//chemins
		val idfPath = "./data/idf_data.csv"
		val pm10Path = "./data/pm10.csv"
		val pm25Path = "./data/pm25.csv"

		//Types des attributs
		val schema = StructType(Seq(
			StructField("Identifiant station", StringType, true),
			StructField("Nom de la Station", StringType, true),
			StructField("Nom de la ligne", StringType, true),
			StructField("Niveau de pollution", StringType, true),
			StructField("Recommandation de surveillance", StringType, true),
			StructField("stop_lon", DoubleType, true),
			StructField("stop_lat", DoubleType, true),
			StructField("pollution_air", StringType, true),
		))

		//Lecture des datasets
		val df = spark.read
			.format("csv")
			.option("sep", ";")
			.option("header", true)
			.schema(schema)
			.load(idfPath)

		val pm10_raw = spark.read
			.format("csv")
			.option("header", false)
			.option("sep", ",")
			.option("inferSchema", "false") //typage automatique - à vérifier
			.load(pm10Path)

		val pm25_raw = spark.read
			.format("csv")
			.option("header", false)
			.option("sep", ",")
			.option("inferSchema", "false")
			.load(pm25Path)

		//println("Nombres de lignes avant nettoyage : " + df.count())

		//Suppression des doublons et des valeurs manquantes
		val df_cleaned = df.na.drop().dropDuplicates()
		val pm10_cleaned = pm10_raw.drop().dropDuplicates()
		val pm25_cleaned = pm25_raw.drop().dropDuplicates()

		//println("Nombres de lignes après nettoyage : " + df.count())

		// Liste de stations pour la simulation en temps réel
		val stationsList = df
			.select($"Nom de la Station")
			.distinct()
			.limit(20) // on ne garde que 20 stations pour éviter un tableau énorme
			.as[String]
			.collect()
			.toSeq

		//Affichage
		println("=== 5 premières lignes des dataset ===")
		df.show(5)
		pm10_raw.show(5)
		pm25_raw.show(5)

		/* 1-bis) Restructuration des data PM10 et PM25*/
		// Récupère la 3ᵉ ligne (index 2) pour les noms de colonnes PM10
		val headerRowPm10: Row = pm10_raw.take(3)(2)
		val colNamesPm10: Array[String] = buildUniqueColNames(headerRowPm10, firstName = "datetime")

		// Idem pour PM2.5
		val headerRowPm25: Row = pm25_raw.take(3)(2)
		val colNamesPm25: Array[String] = buildUniqueColNames(headerRowPm25, firstName = "datetime")

		// (optionnel, pour vérifier)
		println("=== Colonnes PM10 ===")
		colNamesPm10.foreach(println)
		println("=== Colonnes PM25 ===")
		colNamesPm25.foreach(println)

		val pm10Data = pm10_raw
			.filter($"_c0".rlike("^\\d{4}-\\d{2}-\\d{2}")) // On ne garde que les lignes dont la 1ʳᵉ colonne commence par AAAA-MM-JJ
			.toDF(colNamesPm10: _*)
			// enlever le 'Z' final et parser en timestamp
			.withColumn("datetime_raw", regexp_replace($"datetime", "Z$", ""))
			.withColumn("datetime", to_timestamp($"datetime_raw", "yyyy-MM-dd HH:mm:ss"))
			.drop("datetime_raw")

		val pm25Data = pm25_raw
			.filter($"_c0".rlike("^\\d{4}-\\d{2}-\\d{2}"))
			.toDF(colNamesPm25: _*)
			.withColumn("datetime_raw", regexp_replace($"datetime", "Z$", ""))
			.withColumn("datetime", to_timestamp($"datetime_raw", "yyyy-MM-dd HH:mm:ss"))
			.drop("datetime_raw")

		// Colonnes station (tout sauf datetime)
		val pm10StationCols = colNamesPm10.tail	// sans la première "datetime"
		val nPm10 = pm10StationCols.length

		// Expression stack pour Spark SQL
		val pm10StackExpr =
			s"stack($nPm10, " +
				pm10StationCols.map(c => s"'$c', `$c`").mkString(", ") +
				") as (station, pm10)"

		val pm10Long = pm10Data
			.select(
				$"datetime",
				expr(pm10StackExpr)
			)
			.where($"pm10".isNotNull)

		// Passage du format dataset "wide" vers "long" via stack
		val pm25StationCols = colNamesPm25.tail
		val nPm25 = pm25StationCols.length

		val pm25StackExpr =
			s"stack($nPm25, " +
				pm25StationCols.map(c => s"'$c', `$c`").mkString(", ") +
				") as (station, pm25)"

		val pm25Long = pm25Data
			.select(
				$"datetime",
				expr(pm25StackExpr)
			)
			.where($"pm25".isNotNull)

		val pollution = pm10Long
			.join(pm25Long, Seq("datetime", "station"), "outer")

		pollution.show(20, truncate = false)

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
			val ligne	 = row.getAs[String]("ligne")
			val lon		 = row.getAs[Double]("stop_lon")
			val lat		 = row.getAs[Double]("stop_lat")

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
			when($"Niveau de pollution" === "FAIBLE",	 lit(1))
				.when($"Niveau de pollution" === "MOYENNE", lit(2))
				.when($"Niveau de pollution" === "ELEVE",	 lit(3))
				.when($"Niveau de pollution" === "station aérienne",	lit(0)) // ou null si tu préfères
				.otherwise(lit(null).cast("int"))
		)

		//a) Identification des stations les plus exposées les plus pollués
		val stationsExpo = dfScored
			.groupBy("Nom de la Station", "Nom de la ligne")
			.agg(
				avg("score_pollution").as("score_moyen"),
				max("score_pollution").as("score_max")
			)
			.orderBy(desc("score_moyen"), desc("score_max"))

		println("=== Stations les plus exposées ===")
		stationsExpo.show(20, truncate = false)

		//b) Détection des pics horaires et périodes critiques (utilise un dataset complémentaire)

		// ===== Détection de pics de pollution (PM10) =====
		// On ne garde que les lignes où PM10 est défini
		val pm10TS = pollution
			.filter($"pm10".isNotNull)
			.select($"datetime", $"station", $"pm10")
			.orderBy($"station", $"datetime")

		// Fenêtre de 24 points vers l'arrière (par station)
		// Si tes données sont horaires, cela correspond à ~24h
		val w24hPm10 = Window
			.partitionBy("station")
			.orderBy("datetime")
			.rowsBetween(-23, 0)	 // la ligne courante + les 23 précédentes

		// Moyenne et écart-type glissants
		val pm10WithStats = pm10TS
			.withColumn("pm10_mean_24h", avg($"pm10").over(w24hPm10))
			.withColumn("pm10_std_24h", stddev_pop($"pm10").over(w24hPm10))
			.withColumn("pm10_zscore",
				($"pm10" - $"pm10_mean_24h") / $"pm10_std_24h"
			)

		// Un "pic" : z-score > 2.5 et std > 0 (pour éviter division par zéro)
		val pm10Peaks = pm10WithStats
			.filter($"pm10_std_24h" > 0 && $"pm10_zscore" >= 2.5)
			.orderBy($"pm10_zscore".desc)

		// Afficher les 50 plus gros pics PM10
		println("=== Pics de pollution PM10 (z-score >= 2.5) ===")
		pm10Peaks.show(50, truncate = false)

		// ===== Détection de pics de pollution (PM2.5) =====

		val pm25TS = pollution
			.filter($"pm25".isNotNull)
			.select($"datetime", $"station", $"pm25")
			.orderBy($"station", $"datetime")

		val w24hPm25 = Window
			.partitionBy("station")
			.orderBy("datetime")
			.rowsBetween(-23, 0)

		val pm25WithStats = pm25TS
			.withColumn("pm25_mean_24h", avg($"pm25").over(w24hPm25))
			.withColumn("pm25_std_24h", stddev_pop($"pm25").over(w24hPm25))
			.withColumn("pm25_zscore",
				($"pm25" - $"pm25_mean_24h") / $"pm25_std_24h"
			)

		val pm25Peaks = pm25WithStats
			.filter($"pm25_std_24h" > 0 && $"pm25_zscore" >= 2.5)
			.orderBy($"pm25_zscore".desc)

		println("=== Pics de pollution PM2.5 (z-score >= 2.5) ===")
		pm25Peaks.show(50, truncate = false)

		//c) Création d'un indicateur global de pollution
		val global = dfScored
			.agg(avg("score_pollution").as("score_moyen"))
			.withColumn("indice_global_0_100", $"score_moyen" / 3 * 100)	// 3 = score max théorique

		println("=== Indicateur global de pollution ===")
		global.show(false)

		//d) Détection automatique des anomalies dans les données
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

		/*
			6) Traitement en temps réel
			- Simulation de flux de données provenant de capteurs en continu.
			- Application de transformations fonctionnelles sur ces flux.
			- Détection et signalement automatique des anomalies.
		*/

		import org.apache.spark.sql.functions.{window, stddev_pop}

		println("=== Démarrage de la simulation temps réel (Z-score) ===")

		// 1) Flux "capteurs" simulé avec la source rate
		val rawStream = spark.readStream
			.format("rate")
			.option("rowsPerSecond", 10)   // tu peux augmenter / diminuer
			.load()                        // colonnes : timestamp, value

		// On réutilise une liste de stations (définie plus haut dans ton code)
		// stationsList : Seq[String]
		val stationsArrayCol = array(stationsList.map(lit(_)):_*)

		val sensorStream = rawStream
			.withColumn("datetime", $"timestamp")
			// station choisie en fonction de value % nbStations
			.withColumn("station_index",
			($"value" % size(stationsArrayCol)).cast("int")
			)
			.withColumn("station",
			element_at(stationsArrayCol, $"station_index" + 1)
			)
			// PM10 et PM25 simulées (distributions uniformes)
			.withColumn("pm10", expr("20 + rand() * 80"))  // ≈ [20, 100]
			.withColumn("pm25", expr("10 + rand() * 50"))  // ≈ [10, 60]
			.select("datetime", "station", "pm10", "pm25")
	
		// 2) Fenêtres temporelles par station
		// Fenêtre courte (30 s) glissant toutes les 10 s pour voir rapidement des résultats
		val windowedStats = sensorStream
			// .withWatermark("datetime", "10 minutes") // tu pourras le remettre si tu veux parler de retard
			.groupBy(
				window($"datetime", "30 seconds", "10 seconds"),
				$"station"
			)
			.agg(
				avg($"pm10").as("pm10_mean"),
				stddev_pop($"pm10").as("pm10_std"),
				max($"pm10").as("pm10_max"),
				avg($"pm25").as("pm25_mean"),
				stddev_pop($"pm25").as("pm25_std"),
				max($"pm25").as("pm25_max")
			)
			// 3) Z-score sur le max de la fenêtre (même idée que dans la partie batch)
			.withColumn(
				"pm10_zscore",
				when($"pm10_std" > 0, ($"pm10_max" - $"pm10_mean") / $"pm10_std")
					.otherwise(lit(0.0))
			)
			.withColumn(
				"pm25_zscore",
				when($"pm25_std" > 0, ($"pm25_max" - $"pm25_mean") / $"pm25_std")
					.otherwise(lit(0.0))
			)
	
		// 4) Anomalies : fenêtres où le max s'écarte fortement de la moyenne
		// Seuil "gentil" (1.0) pour voir quelque chose en simulation
		// z-score plus agressif
		val anomaliesStream = windowedStats
			.filter($"pm10_zscore" >= 1.75 || $"pm25_zscore" >= 1.75)
			.select(
				$"window.start".as("window_start"),
				$"window.end".as("window_end"),
				$"station",
				$"pm10_max",
				$"pm10_mean",
				$"pm10_std",
				$"pm10_zscore",
				$"pm25_max",
				$"pm25_mean",
				$"pm25_std",
				$"pm25_zscore"
			)
	
		// 5) Sortie console en temps réel
		val query = anomaliesStream.writeStream
			.format("console")
			.outputMode("update")      // pas de sort global, donc "update" OK
			.option("truncate", "false")
			.option("numRows", 50)
			.start()

		query.awaitTermination()

		//Arrêt de Spark
		spark.stop()
	}
}
