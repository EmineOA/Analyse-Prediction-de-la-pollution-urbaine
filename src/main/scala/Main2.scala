import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{window, stddev_pop}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.regression._
import org.apache.spark.ml.evaluation.RegressionEvaluator

//VM options: --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
//-Dhadoop.home.dir=C:\\Users\\Administrateur\\IdeaProjects\\untitled1\\hadoop
//-Djava.library.path=C:\\Users\\Administrateur\\IdeaProjects\\untitled1\\hadoop\\bin

object Main2 {

	//Ouverture d'une session spark
	val spark = SparkSession.builder()
		.appName("Qualite_Air")
		.master("local[*]")
		.config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")	// Kyro pose des problèmes
		.getOrCreate()

	//Imports complémentaires
	import spark.implicits._
	
	
	
	
	
	def importIndivCSV(hasSchema: Boolean, relPath: String): DataFrame={	// Import des fichiers CSV individuellement
		if (hasSchema){					// idf_data
			spark.read
				.format("csv")
				.option("sep", ";")
				.option("header", true)
				.schema(			// Types des attributs de idf_data
					StructType(Seq(
						StructField("Identifiant station", StringType, true),
						StructField("Nom de la Station", StringType, true),
						StructField("Nom de la ligne", StringType, true),
						StructField("Niveau de pollution", StringType, true),
						StructField("Recommandation de surveillance", StringType, true),
						StructField("stop_lon", DoubleType, true),
						StructField("stop_lat", DoubleType, true),
						StructField("pollution_air", StringType, true),
					))
				)		
				.load(relPath)
				.na.drop()			// Suppression des observations avec valeurs vides
				.dropDuplicates()		// Suppression des observations en double
		}
		else{		// pm10 et pm25
			spark.read
				.format("csv")
				.option("sep", ",")
				.option("header", false)
				.load(relPath)
		}
	}
	
	
	
	def buildUniqueColNames(pm: DataFrame): Array[String] = {				// Transformation des noms 
		val seen = scala.collection.mutable.Map[String, Int]()

		// Récupère la 3ᵉ ligne (index 2) pour les noms de colonnes pour pm10 et pm25
		pm.take(3).last.toSeq.zipWithIndex.map {
			case (value, idx) =>
				if (idx == 0) {
					"datetime"		// Valeur de la première en-tête
				}
				else {
					val count = seen.getOrElse(Option(value).map(_.toString.trim).getOrElse(""), 0)
					seen.update(Option(value).map(_.toString.trim).getOrElse(""), count + 1)

					if (count == 0){
						Option(value).map(_.toString.trim).getOrElse("")			// première fois -> "RD934"
					}
					else{
						Option(value).map(_.toString.trim).getOrElse("") + s"_$count"		// deuxième fois -> "RD934_1", puis "RD934_2", etc.
					}
				}
		}.toArray
	}
	
	
	
	def wideToLong(pm: DataFrame): DataFrame={						// Passage du format dataset "wide" vers "long" via stack
		val colNames: Array[String] = buildUniqueColNames(pm)
		
		// Affichage des noms colonnes créés
		println("\n\n\n")
		colNames.foreach(println)
		
		// Nombre de colonnes sans datetime
		val nCol = colNames.length - 1
		
		pm
			.filter($"_c0".rlike("^\\d{4}-\\d{2}-\\d{2}"))				// On ne garde que les lignes dont la 1ʳᵉ colonne commence par AAAA-MM-JJ
			.toDF(colNames: _*)
			.withColumn("datetime_raw", regexp_replace($"datetime", "Z$", ""))	// enlever le 'Z' final et parser en timestamp
			.withColumn("datetime", to_timestamp($"datetime_raw", "yyyy-MM-dd HH:mm:ss"))
			.drop("datetime_raw")
			.select(
				$"datetime",
				expr(
					s"stack($nCol, " +
					colNames.tail.map(c => s"'$c', `$c`").mkString(", ") +
					") as (station, pm)"
				)
			)
			.where($"pm".isNotNull)
	}
	
	
	
	def importCSV(): (DataFrame, DataFrame, DataFrame)={				// Import des fichiers CSV idf_data.csv, pm10.csv et pm25.csv
		// Retourne trois dataframes à partir des csv
		(
			importIndivCSV(true, "./data/idf_data.csv"),
			wideToLong(importIndivCSV(false, "./data/pm10.csv")),
			wideToLong(importIndivCSV(false, "./data/pm25.csv"))
		)
	}
	
	
	
	
	
	def rankByDist(statCentre: Row, idf: DataFrame)={				// Renvoie le dataframe trié par distance par rapport à la station statCentre
		idf
			.select(
				$"Nom de la Station".as("station"),
				$"Nom de la ligne".as("ligne"),
				$"stop_lon",
				$"stop_lat"
			)
			.rdd
			.map { row =>
				val station = row.getAs[String]("station")
				val ligne	 = row.getAs[String]("ligne")
				val lon		 = row.getAs[Double]("stop_lon")
				val lat		 = row.getAs[Double]("stop_lat")

				val dx = lon - statCentre.getAs[Double]("stop_lon")
				val dy = lat - statCentre.getAs[Double]("stop_lat")
				val dist = math.sqrt(dx*dx + dy*dy)

				(dist, station, ligne, lon, lat)
			}
			.sortBy(_._1)
			.toDF(
				"distance",
				"station",
				"ligne",
				"stop_lon",
				"stop_lat"
			)
			.show(20, truncate = false)
	}
	
	
	
	

	def lineNames(idf: DataFrame)={			// Récupération du nom des lignes en IDF
		idf
			.select($"Nom de la ligne".as[String])
			.as[String]
			.flatMap(line => Option(line))
			.distinct()
			.show(200, truncate = false)
	}
	
	

	def descStats(idf: DataFrame)={			// Statistiques descriptives : Moyenne, maximum et minimum sur latitude et longitude
		idf
			.agg(
				avg("stop_lon"),
				min("stop_lon"),
				max("stop_lon")
			)
			.show()
			
		idf
			.agg(
				avg("stop_lat"),
				min("stop_lat"),
				max("stop_lat")
			)
			.show()
	}
	
	
	
	
	
	def qualiToQuanti(idf: DataFrame): DataFrame={					// Variable qualitative pollution -> Variable quantitative
		idf.withColumn(
			"score_pollution",
			when($"Niveau de pollution" === "FAIBLE", lit(1))
			.when($"Niveau de pollution" === "MOYENNE", lit(2))
			.when($"Niveau de pollution" === "ELEVE", lit(3))
			.when($"Niveau de pollution" === "station aérienne", lit(0))	// Stations aériennes non prises en compte
			.otherwise(lit(null).cast("int"))
		)
	}
	
	
	
	def mostPolluted(idf: DataFrame)={						// Identification des stations les plus polluées
		idf
			.groupBy("Nom de la Station", "Nom de la ligne")
			.agg(
				avg("score_pollution").as("score_moyen"),
				max("score_pollution").as("score_max")
			)
			.orderBy(
				desc("score_moyen"),
				desc("score_max")
			)
			.show(20, truncate = false)
	}
	
	
	
	def pollutionPeaks(pm: DataFrame, window: WindowSpec)={				// Affichage des pics de pollution
		pm
			.select(
				$"datetime",
				$"station",
				$"pm"
			)
			.orderBy(
				$"station",
				$"datetime"
			)
			.withColumn(							// Calcul de la moyenne
				"pm_mean_24h",
				avg($"pm").over(window)
			)
			.withColumn(							// Calcul de l'écart-type
				"pm_std_24h",
				stddev_pop($"pm").over(window)
			)
			.withColumn(							// z-score est la valeur centrée réduite
				"pm_zscore",
				($"pm" - $"pm_mean_24h") / $"pm_std_24h"
			)
			.filter($"pm_std_24h" > 0 && abs($"pm_zscore") >= 2.5)		// Un "pic" : z-score > 2.5 et std > 0 (pour éviter division par zéro)
			.orderBy(abs($"pm_zscore").desc)
			.show(50, truncate = false)
	}
	
	
	
	

	def main(args: Array[String]): Unit = {
	/*
		1) Ingestion et préparation des données
			- lecture des fichiers CSV ou JSON en Spark;
			- suppression des doublons et des valeurs manquantes
	*/
		
		
		
		// Import des fichiers CSV idf_data.csv, pm10.csv et pm25.csv
		val (idf, pm10, pm25) = importCSV()

		//Affichage des datasets de base
		println("=== 5 premières lignes des dataset ===")
		idf.show(5)
		pm10.show(5)
		pm25.show(5)
		
		
		
		
		
	/*
		2) Transformation et exploration fonctionnelle
			- Utilisation de map, filter et flatmap pour transformer les données;
			- Calcul de statistiques par station ou par ligne (moyenne, maximum, minimum);
			- Extraction de variables temporelles pertinentes (heure, jour, mois);
	*/
		
		
		
		// Filter - Filtrage selon la longitude
		println("=== Données avec \"stop_lon\" > 2.5 ===")
		idf
			.filter($"stop_lon" > 2.5)
			.show(10)
		
		

		// Map - Stations les plus proches de Cergy Préfecture
		val cergy = idf
			.filter($"Nom de la Station" === "Cergy Préfecture")
			.select("stop_lon", "stop_lat")
			.head()
		val cergyLon = cergy.getAs[Double]("stop_lon")
		val cergyLat = cergy.getAs[Double]("stop_lat")
		println(s"Cergy Préfecture : longitude = $cergyLon, latitude = $cergyLat")

		println("=== Stations les plus proches de Cergy Préfecture ===")
		rankByDist(cergy, idf)



		// FlatMap - récupération du nom des lignes en IDF
		lineNames(idf)
		
		

		// Statistiques descriptives : Moyenne, maximum et minimum (latitude et longitude)
		print("=== Moyenne, maximum, minimum ===")
		descStats(idf)
		




	/*
		3) Analyse approfondie
			- Identification des stations les plus exposées à la pollution
			- Détection des pics horaires et périodes critiques
			- Création d'un indicateur global de pollution
			- Détection automatique des anomalies dans les données
	*/



		// Variable qualitative pollution -> Variable quantitative
		val idfQuanti = qualiToQuanti(idf)
		


		// a) Identification des stations les plus polluées
		println("=== Stations les plus polluées ===")
		mostPolluted(idfQuanti)
		
		

		// b) Détection des pics horaires et périodes critiques (utilise un dataset complémentaire)

		// ===== Détection de pics de pollution (PM10) =====
		
		// Fenêtre de 24 points vers l'arrière (par station)
		// Si tes données sont horaires, cela correspond à ~24h
		val w24h = Window
			.partitionBy("station")
			.orderBy("datetime")
			.rowsBetween(-23, 0)	 // la ligne courante + les 23 précédentes



		// Afficher les 50 plus gros pics PM10
		println("=== Pics de pollution PM10 (z-score >= 2.5) ===")
		pollutionPeaks(pm10, w24h)



		// Afficher les 50 plus gros pics PM2.5
		println("=== Pics de pollution PM2.5 (z-score >= 2.5) ===")
		pollutionPeaks(pm25, w24h)
		
		

		// c) Création d'un indicateur global de pollution
		println("=== Indicateur global de pollution ===")
		val global = idfQuanti
			.agg(avg("score_pollution").as("score_moyen"))
			.withColumn("indice_global_0_100", $"score_moyen" / 3 * 100)	// 3 = score max théorique
			.show(truncate = false)



		// d) Détection automatique des anomalies dans les données
		//Stations "outliers" par rapport à leur ligne
		val statsLigne = idfQuanti
			.groupBy("Nom de la ligne")
			.agg(
				avg("score_pollution").as("moy_ligne"),
				stddev("score_pollution").as("sd_ligne")
			)

		val idfZ = idfQuanti
			.join(statsLigne, "Nom de la ligne")
			.withColumn("z_score",
				($"score_pollution" - $"moy_ligne") / $"sd_ligne"
			)

		val anomaliesStations = idfZ.filter($"z_score" > 1.5) // seuil à ajuster

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
			
		// PARTIE 5

		// 1) Ajout des variables temporelles sur pm10 et sur pm25
		val pm10Feat = pm10
			.withColumn("hour", hour(col("datetime")))
			.withColumn("dayOfWeek", dayofweek(col("datetime")))
			.withColumn("month", month(col("datetime")))
			.withColumn("label", col("pm").cast("double"))
			
		val pm25Feat = pm25
			.withColumn("hour", hour(col("datetime")))
			.withColumn("dayOfWeek", dayofweek(col("datetime")))
			.withColumn("month", month(col("datetime")))
			.withColumn("label", col("pm").cast("double"))
			
		// Découpage en training set et test set
		val Array(trainPm10, testPm10) = pm10Feat.randomSplit(Array(0.7, 0.3), seed = 42)
		val Array(trainPm25, testPm25) = pm25Feat.randomSplit(Array(0.7, 0.3), seed = 42)
			
			

		// 2) Définition des colonnes features
		val categoricalCols = Array("station")  // tu pourras ajouter "Nom de la ligne" si tu l’as jointe
		val numericCols = Array(
			"hour",
			"dayOfWeek",
			"month"
		)
		
		

		// 3) Indexers pour les colonnes catégorielles
		val indexers = new StringIndexer()
			.setInputCol("station")
			.setOutputCol("stationIndex")



		// 4) OneHotEncoder sur ces index
		val encoder = new OneHotEncoder()
			.setInputCol("stationIndex")
			.setOutputCol("stationVec")



		// 5) VectorAssembler vers "features"
		val assembler = new VectorAssembler()
			.setInputCols(numericCols ++ Array("stationVec"))
			.setOutputCol("features")
	
	
	
	/*
		// 6) Pipeline de transformation
		val featureStages: Array[PipelineStage] = Array(indexers, encoder, assembler)

		val featurePipeline = new Pipeline()
			.setStages(featureStages)

		// 7) Application du pipeline
		val featuredData = featurePipeline
			.fit(pm10Feat)
			.transform(pm10Feat)
			.select("hour", "dayOfWeek", "month", "stationVec", "label", "features")
			
		featuredData.show()
	*/
	
	
		
		pm10Feat.show()
		pm25Feat.show()
		
		
		
		
		
		// Evaluation ?
		val evaluator = new RegressionEvaluator()
			.setLabelCol("label")
			.setPredictionCol("label")
			.setMetricName("rmse")
			
			

		// 1) Régression linéaire
		val lr = new LinearRegression()
			.setLabelCol("label")
			.setFeaturesCol("features")

		val lrPipeline = new Pipeline().setStages(Array(indexers, encoder, assembler, lr))
		
		val lrPredPm10 = lrPipeline.fit(trainPm10).transform(testPm10)
		val lrPredPm25 = lrPipeline.fit(trainPm25).transform(testPm25)

		val lrRmsePm10 = evaluator.evaluate(lrPredPm10)
		println(s"LinearRegression RMSE on pm10 = $lrRmsePm10")
		val lrRmsePm25 = evaluator.evaluate(lrPredPm25)
		println(s"LinearRegression RMSE on pm25 = $lrRmsePm25")
		
		
		
		// 2) Arbre de décision (régression)
		val dt = new DecisionTreeRegressor()
			.setLabelCol("label")
			.setFeaturesCol("features")

		val dtPipeline = new Pipeline().setStages(Array(indexers, encoder, assembler, dt))
		
		val dtPredPm10 = dtPipeline.fit(trainPm10).transform(testPm10)
		val dtPredPm25 = dtPipeline.fit(trainPm25).transform(testPm25)
		
		val dtRmsePm10 = evaluator.evaluate(dtPredPm10)
		println(s"DecisionTreeRegressor RMSE = $dtRmsePm10")
		val dtRmsePm25 = evaluator.evaluate(dtPredPm25)
		println(s"DecisionTreeRegressor RMSE = $dtRmsePm25")

		// 3) Forêt aléatoire (régression)
		val rf = new RandomForestRegressor()
			.setLabelCol("label")
			.setFeaturesCol("features")
			.setNumTrees(50)

		val rfPipeline = new Pipeline().setStages(Array(indexers, encoder, assembler, rf))
		
		val rfPredPm10 = rfPipeline.fit(trainPm10).transform(testPm10)
		val rfPredPm25 = rfPipeline.fit(trainPm25).transform(testPm25)
		
		val rfRmsePm10 = evaluator.evaluate(rfPredPm10)
		println(s"RandomForestRegressor RMSE = $rfRmsePm10")
		val rfRmsePm25 = evaluator.evaluate(rfPredPm25)
		println(s"RandomForestRegressor RMSE = $rfRmsePm25")
		
		
		
	/*
		/*
			6) Traitement en temps réel
			- Simulation de flux de données provenant de capteurs en continu.
			- Application de transformations fonctionnelles sur ces flux.
			- Détection et signalement automatique des anomalies.
		*/


		println("=== Démarrage de la simulation temps réel (Z-score) ===")

		// 1) Flux "capteurs" simulé avec la source rate
		val rawStream = spark.readStream
			.format("rate")
			.option("rowsPerSecond", 10)   // tu peux augmenter / diminuer
			.load()                        // colonnes : timestamp, value

		// On réutilise une liste de stations (définie plus haut dans ton code)
		// stationsList : Seq[String]
		

		// Liste de stations pour la simulation en temps réel
		val stationsList = idf
			.select($"Nom de la Station")
			.distinct()
			.limit(20) // on ne garde que 20 stations pour éviter un tableau énorme
			.as[String]
			.collect()
			.toSeq
		
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
	*/

		//Arrêt de Spark
		spark.stop()
	}
}
