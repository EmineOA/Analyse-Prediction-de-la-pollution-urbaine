.PHONY: all
all:
	spark-submit --class Main --master local[*] --conf "spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=file:./src/main/resources/log4j2.properties" target/scala-2.12/projspark_2.12-0.1.0-SNAPSHOT.jar
