# Instructions

## Pour Windows:
Télécharger Hadoop 3.x.x via ce repo par exemple: https://github.com/cdarlint/winutils/tree/master/hadoop-3.0.1
L'insérer dans le dossier IntelliJ Idea
VM: -Dhadoop.home.dir=C:\Users\Administrateur\IdeaProjects\untitled1\hadoop -Djava.library.path=C:\Users\Administrateur\IdeaProjects\untitled1\hadoop\bin 
### Si utilisation de JDK 17
mettre ceci dans les options VM:
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED 

## Pour Linux
aucune manipulation à faire car cet OS est trop bien
