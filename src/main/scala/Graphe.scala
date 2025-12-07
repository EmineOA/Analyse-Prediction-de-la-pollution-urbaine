import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.graphx._
import java.io._

object Graphe {

  // Reconstruit des noms de colonnes uniques à partir de la 3e ligne de pm10/pm25
  def buildUniqueColNames(headerRow: Row, firstName: String = "datetime"): Array[String] = {
    val seen = scala.collection.mutable.Map[String, Int]()
    headerRow.toSeq.zipWithIndex.map {
      case (value, idx) =>
        if (idx == 0) {
          firstName
        } else {
          val raw  = Option(value).map(_.toString.trim).getOrElse("")
          val base =
            if (raw.isEmpty) s"col_$idx"
            else raw.replaceAll("[^A-Za-z0-9_-]", "_")

          val count = seen.getOrElse(base, 0)
          seen.update(base, count + 1)

          if (count == 0) base else s"${base}_$count"
        }
    }.toArray
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Graphe")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // === 1. Définition lignes ⇒ stations ===
    val lignes: Map[String, List[String]] = Map(
      "1" -> List("La Défense - Grande Arche","Esplanade de la Défense","Pont de Neuilly",
        "Les Sablons","Porte Maillot","Argentine","Charles de Gaulle - Étoile","George V",
        "Franklin D. Roosevelt","Champs-Élysées - Clemenceau","Concorde","Tuileries",
        "Palais Royal - Musée du Louvre","Louvre - Rivoli","Châtelet","Hôtel de Ville",
        "Saint-Paul","Bastille","Gare de Lyon","Reuilly - Diderot","Nation",
        "Porte de Vincennes","Saint-Mandé","Bérault","Château de Vincennes"),

      "2" -> List("Porte Dauphine", "Victor Hugo", "Charles de Gaulle - Étoile", "Ternes",
        "Courcelles", "Monceau", "Villiers", "Rome", "Place de Clichy",
        "Blanche", "Pigalle", "Anvers", "Barbès - Rochechouart", "La Chapelle",
        "Stalingrad", "Jaurès", "Colonel Fabien", "Belleville", "Couronnes",
        "Ménilmontant", "Rue Saint-Maur", "Parmentier", "Richard-Lenoir",
        "Bastille", "Bréguet - Sabin", "Chemin Vert", "Saint-Ambroise",
        "Voltaire", "Charonne", "Philippe Auguste", "Alexandre Dumas",
        "Avron", "Nation"),

      "3" -> List("Pont de Levallois - Bécon", "Anatole France", "Louise Michel", "Porte de Champerret",
        "Pereire", "Wagram", "Malesherbes", "Villiers", "Europe", "Saint-Lazare",
        "Havre - Caumartin", "Opéra", "Quatre Septembre", "Bourse", "Sentier",
        "Réaumur - Sébastopol", "Arts et Métiers", "Temple", "République", "Parmentier",
        "Rue Saint-Maur", "Père Lachaise", "Gambetta", "Porte de Bagnolet", "Gallieni"),

      "3bis" -> List("Porte des Lilas", "Saint-Fargeau", "Pelleport", "Gambetta"),

      "4" -> List("Porte de Clignancourt", "Simplon", "Marcadet - Poissonniers", "Château Rouge",
        "Barbès - Rochechouart", "Gare du Nord", "Gare de l'Est", "Château d'Eau",
        "Strasbourg - Saint-Denis", "Réaumur - Sébastopol", "Étienne Marcel", "Les Halles",
        "Châtelet", "Cité", "Saint-Michel", "Odéon", "Saint-Germain-des-Prés",
        "Saint-Sulpice", "Saint-Placide", "Montparnasse - Bienvenüe", "Vavin", "Raspail",
        "Denfert-Rochereau", "Mouton-Duvernet", "Alésia", "Porte d'Orléans",
        "Mairie de Montrouge", "Barbara", "Bagneux - Lucie Aubrac"),

      "5" -> List("Bobigny - Pablo Picasso", "Bobigny - Pantin - Raymond Queneau", "Église de Pantin",
        "Hoche", "Porte de Pantin", "Ourcq", "Laumière", "Jaurès", "Stalingrad", "Gare du Nord",
        "Gare de l'Est", "Jacques Bonsergent", "République", "Oberkampf", "Richard-Lenoir",
        "Bréguet - Sabin", "Bastille", "Quai de la Rapée", "Gare d'Austerlitz",
        "Saint-Marcel", "Campo-Formio", "Place d'Italie"),

      "6" -> List("Charles de Gaulle - Étoile", "Kléber", "Boissière", "Trocadéro", "Passy",
        "Bir-Hakeim", "Dupleix", "La Motte-Picquet - Grenelle", "Cambronne", "Sèvres - Lecourbe",
        "Pasteur", "Montparnasse - Bienvenüe", "Edgar Quinet", "Raspail", "Denfert-Rochereau",
        "Saint-Jacques", "Glacière", "Corvisart", "Place d'Italie", "Nationale",
        "Chevaleret", "Quai de la Gare", "Bercy", "Dugommier", "Daumesnil", "Bel-Air",
        "Picpus", "Nation"),

      "7" -> List("La Courneuve - 8 Mai 1945", "Fort d'Aubervilliers",
        "Aubervilliers - Pantin - Quatre Chemins", "Porte de la Villette", "Corentin Cariou",
        "Crimée", "Riquet", "Stalingrad", "Louis Blanc", "Château-Landon",
        "Gare de l'Est", "Poissonnière", "Cadet", "Le Peletier",
        "Chaussée d'Antin - La Fayette", "Opéra", "Pyramides",
        "Palais Royal - Musée du Louvre", "Pont Neuf", "Châtelet", "Pont Marie",
        "Sully - Morland", "Jussieu", "Place Monge", "Censier - Daubenton",
        "Les Gobelins", "Place d'Italie", "Tolbiac", "Maison Blanche",
        "Porte d'Italie", "Porte de Choisy", "Porte d'Ivry", "Pierre et Marie Curie",
        "Mairie d'Ivry"),

      "7bis" -> List("Louis Blanc","Jaurès", "Bolivar", "Buttes Chaumont","Botzaris","Place des Fêtes",
        "Pré Saint-Gervais", "Danube"),

      "8" -> List("Balard", "Lourmel", "Boucicaut", "Félix Faure", "Commerce",
        "La Motte-Picquet - Grenelle", "École Militaire", "La Tour-Maubourg",
        "Invalides", "Concorde", "Madeleine", "Opéra", "Richelieu - Drouot",
        "Grands Boulevards", "Bonne Nouvelle", "Strasbourg - Saint-Denis", "République",
        "Filles du Calvaire", "Saint-Sébastien - Froissart", "Chemin Vert", "Bastille",
        "Ledru-Rollin", "Faidherbe - Chaligny", "Reuilly - Diderot", "Montgallet",
        "Daumesnil", "Michel Bizot", "Porte Dorée", "Porte de Charenton", "Liberté",
        "Charenton - Écoles", "École Vétérinaire de Maisons-Alfort",
        "Maisons-Alfort - Stade", "Maisons-Alfort - Les Juilliottes",
        "Créteil - L'Échat", "Créteil - Université", "Créteil - Préfecture"),

      "9" -> List("Pont de Sèvres", "Billancourt", "Marcel Sembat", "Porte de Saint-Cloud",
        "Exelmans", "Michel-Ange - Molitor", "Michel-Ange - Auteuil", "Jasmin",
        "Ranelagh", "La Muette", "Rue de la Pompe", "Trocadéro", "Iéna",
        "Alma - Marceau", "Franklin D. Roosevelt", "Saint-Philippe du Roule",
        "Miromesnil", "Saint-Augustin", "Havre - Caumartin",
        "Chaussée d'Antin - La Fayette", "Richelieu - Drouot", "Grands Boulevards",
        "Bonne Nouvelle", "Strasbourg - Saint-Denis", "République", "Oberkampf",
        "Saint-Ambroise", "Voltaire", "Charonne", "Rue des Boulets", "Nation",
        "Buzenval", "Maraîchers", "Porte de Montreuil", "Robespierre",
        "Croix de Chavaux", "Mairie de Montreuil"),

      "10" -> List("Boulogne - Pont de Saint-Cloud", "Boulogne - Jean Jaurès",
        "Michel-Ange - Molitor", "Michel-Ange - Auteuil", "Chardon-Lagache",
        "Église d'Auteuil", "Mirabeau", "Javel - André Citroën", "Charles Michels",
        "Avenue Émile Zola", "La Motte-Picquet - Grenelle", "Ségur", "Duroc",
        "Vaneau", "Sèvres - Babylone", "Mabillon", "Odéon", "Cluny - La Sorbonne",
        "Maubert - Mutualité", "Cardinal Lemoine", "Jussieu", "Gare d'Austerlitz"),

      "11" -> List("Châtelet", "Hôtel de Ville", "Rambuteau", "Arts et Métiers", "République",
        "Goncourt", "Belleville", "Pyrénées", "Jourdain", "Place des Fêtes",
        "Télégraphe", "Porte des Lilas", "Mairie des Lilas", "Serge Gainsbourg",
        "Romainville - Carnot", "Montreuil - Hôpital", "La Dhuys", "Coteaux - Beauclair",
        "Rosny-Bois-Perrier"),

      "12" -> List("Aubervilliers - Front Populaire", "Porte de la Chapelle", "Marx Dormoy",
        "Marcadet - Poissonniers", "Jules Joffrin", "Lamarck - Caulaincourt",
        "Abbesses", "Pigalle", "Saint-Georges", "Notre-Dame-de-Lorette",
        "Trinité - d'Estienne d'Orves", "Saint-Lazare", "Madeleine", "Concorde",
        "Assemblée Nationale", "Solférino", "Rue du Bac", "Sèvres - Babylone",
        "Rennes", "Notre-Dame-des-Champs", "Montparnasse - Bienvenüe", "Falguière",
        "Pasteur", "Volontaires", "Vaugirard", "Convention", "Porte de Versailles",
        "Corentin Celton", "Mairie d'Issy"),

      "13_tronc" -> List("La Fourche", "Place de Clichy", "Liège", "Saint-Lazare",
        "Miromesnil", "Champs-Élysées – Clemenceau", "Invalides", "Varenne",
        "Saint-François-Xavier", "Duroc", "Montparnasse – Bienvenüe",
        "Gaîté", "Pernety", "Plaisance", "Porte de Vanves",
        "Malakoff – Plateau de Vanves", "Malakoff – Rue Étienne Dolet",
        "Châtillon – Montrouge"),

      "13_Asnières" -> List("La Fourche", "Brochant", "Porte de Clichy", "Mairie de Clichy",
        "Gabriel Péri", "Les Agnettes", "Les Courtilles"),

      "13_Saint-Denis" -> List("La Fourche", "Guy Môquet", "Porte de Saint-Ouen", "Garibaldi",
        "Mairie de Saint-Ouen", "Carrefour Pleyel", "Saint-Denis – Porte de Paris",
        "Basilique de Saint-Denis", "Saint-Denis – Université"),

      "14" -> List("Saint-Denis – Pleyel", "Mairie de Saint-Ouen", "Porte de Clichy",
        "Pont Cardinet", "Saint-Lazare", "Madeleine", "Pyramides", "Châtelet",
        "Gare de Lyon", "Bercy", "Cour Saint-Émilion",
        "Bibliothèque François Mitterrand", "Olympiades", "Maison Blanche",
        "Hôpital Bicêtre", "Villejuif – Gustave Roussy", "L'Haÿ-les-Roses",
        "Chevilly-Larue", "Thiais-Orly", "Aéroport d'Orly")

    )

    // === 2. Création IDs ===
    val stations = lignes.values.flatten.toSet
    val stationToId = stations.zipWithIndex.toMap

    val vertices = sc.parallelize(
      stationToId.toSeq.map { case (name, id) => (id.toLong, name) }
    )

    val edges = sc.parallelize(
      lignes.toSeq.flatMap { case (ligne, sts) =>
        sts.zip(sts.tail).map { case (u, v) =>
          Edge(stationToId(u).toLong, stationToId(v).toLong, ligne)
        }
      }
    )

    val graph = Graph(vertices, edges)
    import spark.implicits._

    // =====================================================================
    // 3. Lecture de idf_data.csv et calcul d'un score de pollution par station
    // =====================================================================

    val idfPath = "./data/idf_data.csv"

    // Même schéma que dans ton main.scala
    val schema = StructType(Seq(
      StructField("Identifiant station", StringType, true),
      StructField("Nom de la Station", StringType, true),
      StructField("Nom de la ligne", StringType, true),
      StructField("Niveau de pollution", StringType, true),
      StructField("Recommandation de surveillance", StringType, true),
      StructField("stop_lon", DoubleType, true),
      StructField("stop_lat", DoubleType, true),
      StructField("pollution_air", StringType, true)
    ))

    val df = spark.read
      .format("csv")
      .option("sep", ";")
      .option("header", "true")
      .schema(schema)
      .load(idfPath)

    // Encodage ordinal sur la variable catégorielle "Niveau de pollution"
    val dfScored = df.withColumn(
      "score_pollution",
      when($"Niveau de pollution" === "FAIBLE",   lit(1))
        .when($"Niveau de pollution" === "MOYENNE", lit(2))
        .when($"Niveau de pollution" === "ELEVE",   lit(3))
        .when($"Niveau de pollution" === "station aérienne", lit(0))
        .otherwise(lit(null).cast("int"))
    )

    // Moyenne du score par station
    val pollutionByStation: Map[String, Double] =
      dfScored
        .groupBy($"Nom de la Station".as("station"))
        .agg(avg($"score_pollution").as("pollution_score"))
        .where($"pollution_score".isNotNull)
        .select($"station", $"pollution_score".cast("double"))
        .as[(String, Double)]
        .collect()
        .toMap

    println(s"Stations avec un score de pollution = ${pollutionByStation.size}")

    // =====================================================================
    // 4. Graphe dont l'attribut de sommet = pollution locale initiale
    // =====================================================================

    val pollutionRDD = sc.parallelize(
      pollutionByStation.toSeq.collect {
        case (stationName, value) if stationToId.contains(stationName) =>
          (stationToId(stationName).toLong, value)
      }
    )

    println(s"Stations effectivement mappées sur le graphe = ${pollutionRDD.count()}")

    // graph : Graph[String, String]  (nomStation, ligne)
    // g     : Graph[Double, String]  (pollution, ligne)
    var g: Graph[Double, String] =
      graph.outerJoinVertices(pollutionRDD) {
        case (_, _, pollutionOpt) => pollutionOpt.getOrElse(0.0)
      }

    // =====================================================================
    // 5. Palette de couleurs pour les lignes (pour les arêtes)
    // =====================================================================

    val couleurs = Array(
      "#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd",
      "#8c564b","#e377c2","#7f7f7f","#bcbd22","#17becf",
      "#aec7e8","#ffbb78","#98df8a","#ff9896","#c5b0d5",
      "#c49c94","#f7b6d2","#c7c7c7","#dbdb8d","#9edae5"
    )

    def couleurLigne(l: String): String = {
      val base = l.split("_")(0)
      try {
        val i = base.toInt - 1
        couleurs(i % couleurs.length)
      } catch {
        case _: Throwable => "#000000"
      }
    }

    // =====================================================================
    // 6. Diffusion de la pollution (style Pregel) + DOT par itération
    // =====================================================================

    val alpha   = 0.10   // coefficient de diffusion
    val maxIter = 5      // nombre d'itérations

    val verticesLocal = vertices.collect().toMap   // id -> nom
    val edgesLocal    = edges.collect()

    def writeDotForIteration(iter: Int, currentGraph: Graph[Double, String]): Unit = {
      val scores = currentGraph.vertices.collect().toMap
      val nonZeroVals = scores.values.filter(_ > 0.0)

      if (nonZeroVals.nonEmpty) {
        val minP = nonZeroVals.min
        val maxP = nonZeroVals.max

        println(s"[Iter $iter] minP>0 = $minP, maxP>0 = $maxP")

        def colorFromPoll(x: Double): String = {
          if (x <= 0.0) {
            "#DDDDDD"  // stations sans pollution mesurée
          } else {
            val r0 =
              if (maxP == minP) 0.0
              else (x - minP) / (maxP - minP)

            val r = math.max(0.0, math.min(1.0, r0))
            val red   = (255 * r).toInt
            val green = (255 * (1 - r)).toInt
            f"#$red%02x$green%02x00"  // gradient vert -> rouge
          }
        }

        val fileName = s"metro_iter_$iter.dot"
        val outIter  = new PrintWriter(new File(fileName))

        outIter.println(
          s"""
             |graph metro {
             |  graph [overlap=false,
             |         layout=sfdp,
             |         fontsize=20,
             |         label="Diffusion de la pollution (idf_data) - itération $iter",
             |         labelloc="t",
             |         size="20,20",
             |         dpi=300];
             |  node  [shape=circle, fontsize=8, style=filled, width=0.2, height=0.2];
             |  edge  [penwidth=1];
             |""".stripMargin
        )

        // Noeuds : couleur = pollution propagée
        verticesLocal.foreach { case (id, name) =>
          val label = name.replace("\"", "'")
          val p     = scores.getOrElse(id, 0.0)
          val col   = colorFromPoll(p)
          outIter.println(s"""  $id [label="$label", fillcolor="$col"];""")
        }

        // Arêtes colorées par ligne
        edgesLocal.foreach { e =>
          val col = couleurLigne(e.attr)
          outIter.println(s"""  ${e.srcId} -- ${e.dstId} [color="$col", label="${e.attr}"];""")
        }

        outIter.println("}")
        outIter.close()

        println(s"Fichier DOT généré pour l'itération $iter : $fileName")
      } else {
        println(s"[Iter $iter] Attention : aucune station avec pollution > 0.")
      }
    }

    // Boucle de diffusion (type Pregel "déroulé")
    for (iter <- 0 to maxIter) {
      // 1) Écriture du graphe à l'état courant
      writeDotForIteration(iter, g)

      // 2) Propagation pour l'itération suivante
      if (iter < maxIter) {
        val messages = g.aggregateMessages[Double](
          triplet => {
            // chaque station envoie alpha * sa pollution à ses voisines
            triplet.sendToDst(triplet.srcAttr * alpha)
            triplet.sendToSrc(triplet.dstAttr * alpha)
          },
          _ + _
        )

        g = g.joinVertices(messages) {
          case (_, currentPoll, receivedPoll) => currentPoll + receivedPoll
        }
      }
    }

    println(s"Diffusion terminée. DOT générés : metro_iter_0.dot ... metro_iter_$maxIter.dot")

    spark.stop()

  }
}
