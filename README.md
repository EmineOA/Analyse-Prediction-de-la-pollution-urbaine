<img width="212" height="17" alt="image" src="https://github.com/user-attachments/assets/a6251d61-c38c-43cf-9819-0f0d1f94d9ed" /># Instructions

## Pour Windows:
Télécharger Hadoop 3.x.x via ce repo par exemple: https://github.com/cdarlint/winutils/tree/master/hadoop-3.0.1
L'insérer dans le dossier IntelliJ Idea
VM: -Dhadoop.home.dir=C:\Users\Administrateur\IdeaProjects\untitled1\hadoop -Djava.library.path=C:\Users\Administrateur\IdeaProjects\untitled1\hadoop\bin 
### Si utilisation de JDK 17
mettre ceci dans les options VM:
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED 

## Pour Linux
Aucune manipulation à faire car cet OS est trop bien.


# Modifications dataset
Retirer les colonnes "Incertitude", "Action(s) QAI en cours", "Lien vers les mesures en direct", "Durée des mesures", "Mesures d'amélioration mises en place ou prévues", "air", "actions" pour manque de données.
Retirer les colonnes "Niveau de pollution aux particules", "niveau", "niveau_pollution" car exacte égalité avec la colonne "Niveau de pollution".
Retirer la colonne "point_geo" car concaténation de "stop_lon" et "stop_lat".
