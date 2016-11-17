D:\tools\spark-1.6.3-bin-hadoop2.6\bin\spark-submit --conf spark.network.timeout=10000000 --conf spark.executor.heartbeatInterval=10000000 --master local --deploy-mode client --class com.consumer.Main --name history-analytics  ../target/history-analytics-1.0-SNAPSHOT-jar-with-dependencies.jar
--driver-java-options -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
