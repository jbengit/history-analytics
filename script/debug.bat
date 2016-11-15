spark-submit --master local --deploy-mode client --class com.consumer.Main --name history-analytics  ../target/history-analytics-1.0-SNAPSHOT-jar-with-dependencies.jar
--driver-java-options -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
