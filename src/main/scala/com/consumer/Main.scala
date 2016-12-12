package com.consumer

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD  
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.consumer.java.ConsumerHistoryDeepLearning

//http://ted-gao.blogspot.co.il/2011/09/mixing-scala-and-java-in-project.html
object Main {
  
    var log:Logger = LoggerFactory.getLogger("Main");	  
  
  def main(args: Array[String])
  {
    //val instance = new com.consumer.java.MnistMLPExample();
   // com.consumer.java.MnistMLPExample.run(e)
    //com.consumer.java.HistoryDeepLearning.trainAndTest("mnist");
    
    val rootBasePath = args(0)
    val appName = args(1)
    val action = args(2)
    var actionParameter:String = null;
    if(args.length >= 3)
      actionParameter = args(3)
    
    log.info("***** Launch spark job: " + appName + " ****");
    log.info("***** spark rootBasePath: " + rootBasePath + " ****");
    log.info("***** spark action: " + action + " ****");
    log.info("***** spark action parameters: " + actionParameter + " ****");
    
    val instance = new ConsumerHistoryDeepLearning();
    if(action == "train")
      instance.trainAndTest(rootBasePath, appName, actionParameter);
    else
      instance.loadAndTest(rootBasePath, appName, actionParameter);
    log.info("***** spark shell exit ****");
    
  /*  val conf = new SparkConf().setAppName("Word Counter");
    conf.setMaster("local")
    val sc = new SparkContext(conf);
       
    val textFile = sc.textFile("file:///C:/tmp/data/output/RFM/part-r-00000", 1)
    val tokenizedFileData = textFile.flatMap { line => line.split(" ") }
    val countPrep = tokenizedFileData.map ( word => (word, 1) )
    val counts = countPrep.reduceByKey((accumValue, newValue) => accumValue + newValue)
    val sortedCounts = counts.sortBy(kvPair => kvPair._2, false)
    sortedCounts.saveAsTextFile("file:///C:/tmp/data/spark-output/RFM/ReadMeWordCountViaApp")
   
    */
  }
  
}