package com.consumer

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
//import spark.implicits._
import org.apache.spark.rdd.RDD
import org.slf4j.Logger
import org.apache.spark.sql._
import org.slf4j.LoggerFactory
import com.consumer.java.ConsumerHistoryDeepLearning

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._


//http://ted-gao.blogspot.co.il/2011/09/mixing-scala-and-java-in-project.html
object Main {

  var log: Logger = LoggerFactory.getLogger("Main");

  def main(args: Array[String]) {
    //val instance = new com.consumer.java.MnistMLPExample();
    // com.consumer.java.MnistMLPExample.run(e)
    //com.consumer.java.HistoryDeepLearning.trainAndTest("mnist");

    val rootBasePath = args(0)
    val appName = args(1)
    val action = args(2)
    var actionParameter: String = null;
    if (args.length >= 4)
      actionParameter = args(3)

    log.info("***** Launch spark job: " + appName + " ****");
    log.info("***** spark rootBasePath: " + rootBasePath + " ****");
    log.info("***** spark action: " + action + " ****");
    log.info("***** spark action parameters: " + actionParameter + " ****");

    if(appName == "history")
    {
       val instance = new ConsumerHistoryDeepLearning();
      if (action == "train")
       instance.trainAndTest(rootBasePath, appName, actionParameter);
      else
        instance.loadAndTest(rootBasePath, appName, actionParameter);
    }
    else
    {
      if(appName == "transcript")
      {
        val sc = initSpark(appName)     
        val transcriptRoot = "file:///C:/tmp/data/transcript/agg/";        
        val sqlContext = new SQLContext(sc)
        if(action == "convert")
          createTranscriptParquetFile(appName, transcriptRoot, sqlContext);
        if(action == "sentiment")
        {
      //      createSentimentFile(appName, transcriptRoot, sqlContext);
            showSentimentFile(appName, transcriptRoot, sqlContext)
        }                
      }
    }
    
    
   
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
  
  def initSpark(name:String): SparkContext =
  {    
    	val sparkConf = new SparkConf();       
      sparkConf.setMaster("local[*]");        
      sparkConf.setAppName(name);
      new SparkContext(sparkConf);
  }
  
  def createTranscriptParquetFile(name:String, transcriptRoot:String, sqlContext:SQLContext) =
	{           
      val schema = StructType(Array(
          StructField("SessionId",        IntegerType,false),
          StructField("MemberId",        IntegerType,false),
          StructField("ExpertId",        IntegerType,false),
          StructField("IsMember",        BooleanType,false),
          StructField("Text",          StringType,true),
          StructField("Amount",            FloatType,true),
          StructField("TransactionType",             StringType,true),
          StructField("SessionDateTime",         DateType,true),
          StructField("Order",    LongType,true),
          StructField("SiteID",            IntegerType,true)));
      
      convert(sqlContext, transcriptRoot + "transcript.csv", transcriptRoot + "transcript.parquet", schema, "transcript")
      sqlContext
	 }
  
  
  def convert(sqlContext: SQLContext, inputFilePath: String, outputFilename: String, schema: StructType, tablename: String) {
      val df = sqlContext.read
      .format("csv")
      .option("sep", ",")
      .option("header", true)
      .option("timestampFormat", "dd/MM/yyyy HH:mm:ss")
      .option("dateFormat", "dd/MM/yyyy HH:mm:ss")
      .schema(schema)        
      .load(inputFilePath)                      
      df.write.format("parquet").save(outputFilename)
  }
  
  def showSentimentFile(name:String, transcriptRoot:String, sqlContext:SQLContext)
  {   
      val spark = sqlContext
      import spark.implicits._
      val inputPath = transcriptRoot + "sentiment.parquet"               
      val parquetFileDF = sqlContext.read.parquet(inputPath)
      parquetFileDF.orderBy('avgSentiment).show()                     
  }
  
  def createSentimentFile(name:String, transcriptRoot:String, sqlContext:SQLContext)
  {   
   val inputPath = transcriptRoot + "transcript.parquet"
   val outputPath = transcriptRoot + "sentiment.parquet"    
    
    val spark = sqlContext
   import spark.implicits._
    val parquetFileDF = sqlContext.read.parquet(inputPath)
    parquetFileDF.createOrReplaceTempView(name)           
    val result = sqlContext.sql("SELECT Text, MemberId, ExpertId, SessionId, Amount FROM " + name + " WHERE IsMember = true AND Text is not null")    
    val output = result         
    .select(explode(ssplit('Text)).as('sen), 'MemberId, 'ExpertId, 'SessionId, 'Amount)              
    .select('MemberId, 'ExpertId, 'SessionId, 'Amount, sentiment('sen).as('sentiment))    
    .groupBy('SessionId, 'MemberId, 'ExpertId, 'Amount)
    .agg(avg('sentiment).as('avgSentiment), count('sentiment).as('sentenceCount))    
    .select('SessionId, 'MemberId, 'ExpertId, 'Amount, 'avgSentiment, 'sentenceCount)
    .orderBy('avgSentiment.desc)
    output.write.parquet(outputPath)       
  }   
}