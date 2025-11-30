import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import java.io._

object Graphe {

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

    // === 3. Palette de couleurs façon "tab20" ===
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

    // === 4. Génération DOT ===
    val out = new PrintWriter(new File("metro.dot"))

    out.println("""
                  |graph metro {
                  |  graph [overlap=false, layout=sfdp, fontsize=20, label="Graphe du métro parisien (toutes lignes)"];
                  |  node  [shape=circle, fontsize=10, style=filled, fillcolor="#DDDDDD", width=0.2, height=0.2];
                  |  edge  [penwidth=1];
                  |""".stripMargin)

    // Noeuds
    vertices.collect().foreach { case (id, name) =>
      val label = name.replace("\"", "'")
      out.println(s"""  $id [label="$label"];""")
    }

    // Arêtes colorées
    edges.collect().foreach { e =>
      val col = couleurLigne(e.attr)
      out.println(s"""  ${e.srcId} -- ${e.dstId} [color="$col", label="${e.attr}"];""")
    }

    out.println("}")
    out.close()

    println("Fichier DOT généré : metro.dot")
    println("Génère l’image avec :")
    println("dot -K sfdp -Tpng metro.dot -o metro.png")

    spark.stop()
  }
}