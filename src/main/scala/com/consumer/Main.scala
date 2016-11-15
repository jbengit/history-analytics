package com.consumer

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Main {
  
  def main(args: Array[String])
  {
    
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