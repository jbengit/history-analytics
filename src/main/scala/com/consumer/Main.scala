package com.consumer

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD  

//http://ted-gao.blogspot.co.il/2011/09/mixing-scala-and-java-in-project.html
object Main {
  
  def main(args: Array[String])
  {
    //val instance = new com.consumer.java.MnistMLPExample();
   // com.consumer.java.MnistMLPExample.run()
    //com.consumer.java.HistoryDeepLearning.trainAndTest("mnist");
    com.consumer.java.HistoryDeepLearning.loadAndTest("mnist");
    return
    val conf = new SparkConf().setAppName("Word Counter");
    conf.setMaster("local")
    val sc = new SparkContext(conf);
       
    val textFile = sc.textFile("file:///C:/tmp/data/output/RFM/part-r-00000", 1)
    val tokenizedFileData = textFile.flatMap { line => line.split(" ") }
    val countPrep = tokenizedFileData.map ( word => (word, 1) )
    val counts = countPrep.reduceByKey((accumValue, newValue) => accumValue + newValue)
    val sortedCounts = counts.sortBy(kvPair => kvPair._2, false)
    sortedCounts.saveAsTextFile("file:///C:/tmp/data/spark-output/RFM/ReadMeWordCountViaApp")
  }
  
}