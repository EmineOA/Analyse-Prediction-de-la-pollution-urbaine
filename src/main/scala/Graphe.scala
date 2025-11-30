package graphx

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import java.io.{File, PrintWriter}

object Graphe {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IleDeFrance GraphX")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // --- Chemins (les CSV doivent être dans data) ---
    val stationsPath = "data/idf_data_clean.csv"
    val connPath = "data/stations_connexions.csv"

    println(s"Lecture des fichiers :\n  stations = $stationsPath\n  connexions = $connPath")

    // --- Lecture DataFrames (détecte automatiquement l'entête et le séparateur) ---
    val stationsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(stationsPath)
      .cache()

    val connDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(connPath)
      .cache()

    println("Colonnes stationsDF : " + stationsDF.columns.mkString(", "))
    println("Colonnes connDF : " + connDF.columns.mkString(", "))

    // --- Trouver colonne id/name possible dans stationsDF ---
    val possibleIdCols = Seq("station_id","id","stationid","stationId","ID")
    val possibleNameCols = Seq("name","station_name","station","libelle","nom")

    val idColOpt = stationsDF.columns.find(c => possibleIdCols.contains(c.toLowerCase))
    val nameColOpt = stationsDF.columns.find(c => possibleNameCols.contains(c.toLowerCase))

    // --- Construire mapping stationKey -> longId ---
    // On va accepter soit : fichier stations contient un id numérique unique => on l'utilise
    // Sinon on utilisera le nom (ou toute autre colonne) et on zipWithUniqueId.
    val stationKeyCol = idColOpt.orElse(nameColOpt).getOrElse {
      // fallback: première colonne
      stationsDF.columns.head
    }

    // Build RDD of distinct keys
    val stationKeysRDD = stationsDF.select(stationKeyCol).distinct().na.drop().rdd.map(row => row.get(0).toString)

    // Create mapping String -> Long (unique)
    val keysWithId = stationKeysRDD.zipWithUniqueId().map { case (key, id) => (key, id) }
    val stationMap = keysWithId.collect().toMap // small enough to collect (stations lisibles)
    println(s"Nombre de stations identifiées : ${stationMap.size}")

    // Create vertices RDD for GraphX
    val sc = spark.sparkContext
    val verticesRDD = sc.parallelize(stationMap.toSeq.map { case (k, v) => (v: VertexId, k) })

    // --- Préparer les arêtes (edges) en détectant colonnes from/to ---
    val candidateFrom = Seq("from","origin","station_a","station1","depart","start","src")
    val candidateTo   = Seq("to","destination","station_b","station2","arrivee","end","dst")

    // Find columns in connDF (case-insensitive)
    def findCol(dfCols: Array[String], candidates: Seq[String]) =
      dfCols.find(c => candidates.contains(c.toLowerCase)).orElse(dfCols.headOption)

    val fromCol = findCol(connDF.columns, candidateFrom).get
    val toCol   = {
      // try to find a different column for 'to'
      val maybe = findCol(connDF.columns.filterNot(_ == fromCol), candidateTo)
      maybe.getOrElse(connDF.columns.filterNot(_ == fromCol).headOption.getOrElse(fromCol))
    }

    println(s"Utilisation colonnes connexions : from='$fromCol'  to='$toCol'")

    // Read pairs from connDF and map to vertex ids using stationMap; if keys are numeric ids, prefer matching to stationKeyCol
    val edgesRDD = connDF.select(fromCol, toCol).na.drop().rdd.flatMap { row =>
      try {
        val aKey = row.get(0).toString
        val bKey = row.get(1).toString
        val aOpt = stationMap.get(aKey)
        val bOpt = stationMap.get(bKey)
        (aOpt, bOpt) match {
          case (Some(aId), Some(bId)) =>
            Seq(Edge(aId, bId, 1), Edge(bId, aId, 1)) // graphe non orienté -> deux arêtes
          case _ =>
            // Maybe the connections CSV uses numeric station IDs that correspond to the original stations id column.
            // Try to match by converting to numeric string.
            val aTry = stationMap.get(aKey.trim)
            val bTry = stationMap.get(bKey.trim)
            (aTry, bTry) match {
              case (Some(aId2), Some(bId2)) => Seq(Edge(aId2, bId2, 1), Edge(bId2, aId2, 1))
              case _ =>
                // ignore if cannot map
                Seq.empty[Edge[Int]]
            }
        }
      } catch {
        case _: Throwable => Seq.empty[Edge[Int]]
      }
    }

    println(s"Edges count (raw RDD) : ${edgesRDD.count()}")

    // Build graph
    val defaultVertexAttr = "unknown"
    val graph = Graph(verticesRDD, edgesRDD, defaultVertexAttr)

    println(s"Graph construit : vertices=${graph.numVertices} edges=${graph.numEdges}")

    // Optionnel : afficher quelques sommets et arêtes
    println("Exemples de sommets (id -> label) :")
    graph.vertices.take(20).foreach { case (id, label) => println(s"$id -> $label") }

    println("Exemples d'arêtes (src -> dst) :")
    graph.edges.take(20).foreach(e => println(s"${e.srcId} -> ${e.dstId} (attr=${e.attr})"))

    // --- Ecrire un fichier .dot pour visualisation via Graphviz ---
    val dotPath = "graph_output.dot"
    val pw = new PrintWriter(new File(dotPath))
    try {
      pw.println("graph G {")
      pw.println("  overlap=false;")
      pw.println("  splines=true;")
      // vertices with labels
      graph.vertices.collect().foreach { case (id, label) =>
        // sanitize label
        val lab = label.replace("\"", "\\\"")
        pw.println(s"""  $id [label="$lab", shape=circle];""")
      }
      // edges (undirected -> write once)
      // collect edges, make set of pairs with min,max to avoid duplicates
      val edgePairs = graph.edges.map(e => (math.min(e.srcId, e.dstId), math.max(e.srcId, e.dstId))).distinct().collect()
      edgePairs.foreach { case (a, b) =>
        pw.println(s"  $a -- $b;")
      }
      pw.println("}")
    } finally {
      pw.close()
    }

    println(s"Fichier DOT écrit : $dotPath")
    println("Pour convertir en PNG (Graphviz) : dot -Tpng graph_output.dot -o graph_output.png")
    println("Ou ouvrir graph_output.dot avec un visualiseur GraphViz.")

    // stop
    spark.stop()
  }
}
