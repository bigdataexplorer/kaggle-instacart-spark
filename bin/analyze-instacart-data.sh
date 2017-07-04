wget http://central.maven.org/maven2/com/databricks/spark-csv_2.10/1.3.1/spark-csv_2.10-1.3.1.jar
wget http://central.maven.org/maven2/com/databricks/spark-avro_2.10/2.0.1/spark-avro_2.10-2.0.1.jar
wget http://central.maven.org/maven2/org/apache/commons/commons-csv/1.2/commons-csv-1.2.jar

INPUT_PATH=$1

spark-submit --jars spark-csv_2.10-1.3.1.jar,commons-csv-1.2.jar,spark-avro_2.10-2.0.1.jar --class wsc.bigdata.spark.AnalyzeInstaCartData kaggle-instacart-spark-1.0-SNAPSHOT.jar ${INPUT_PATH}
