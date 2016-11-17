package com.consumer.java;


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.spark.api.TrainingMaster;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistoryDeepLearning {
	  private static final Logger log = LoggerFactory.getLogger(HistoryDeepLearning.class);	  
	    private static boolean useSparkLocal = true;	  
	    private static int batchSizePerWorker = 16;
	  	    
	    private static JavaSparkContext sc = null;
	    
	    private static JavaRDD<DataSet> trainData = null;
	    private static JavaRDD<DataSet> testData = null;
	    private static SparkDl4jMultiLayer network = null;	 
	    private static TrainingMaster tm = null;
	
        //Create network configuration and conduct network training
	    private static MultiLayerConfiguration BuildNetworkConfiguration()
	    {
	    	return new NeuralNetConfiguration.Builder()
            .seed(12345)
            .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).iterations(1)
            .activation("leakyrelu")
            .weightInit(WeightInit.XAVIER)
            .learningRate(0.02)
            .updater(Updater.NESTEROVS).momentum(0.9)
            .regularization(true).l2(1e-4)
            .list()
            .layer(0, new DenseLayer.Builder().nIn(28 * 28).nOut(500).build())
            .layer(1, new DenseLayer.Builder().nIn(500).nOut(100).build())
            .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
                .activation("softmax").nIn(100).nOut(10).build())
            .pretrain(false).backprop(true)
            .build();	    	
	    }
	    
	    private static void initSpark(String name)
	    {
	    	SparkConf sparkConf = new SparkConf();
	        if (useSparkLocal) {
	            sparkConf.setMaster("local[*]");
	        }
	        sparkConf.setAppName(name);
	        sc = new JavaSparkContext(sparkConf);
	    }
	    
	    
	    private static void load(SparkDl4jMultiLayer network) throws Exception
	    {
	    	 //TODO
	    	INDArray params = Nd4j.readTxt("C:\\tmp\\data\\spark-output\\MINST\\params" + 15 + ".txt");
	    	network.getNetwork().setParams(params);
	    	throw new Exception("Not implemented");
	    }
	    
	    public static void loadAndTest(String name) throws Exception
	    {
	    	initSpark(name);
	    	createNetwork();
	    	load(network);	   
	    	createTestDataSet();
	    	test(network);
	    	dispose();
	    	log.info("***** Example Complete *****");	    
	    }
	    
	    private static void createNetwork()
	    {
	    	MultiLayerConfiguration conf = BuildNetworkConfiguration();	        			        
	        tm = BuildTrainingParameters();	        
	        network =  BuildNetwork(conf, tm);		    	
	    }
	    
	    public static void trainAndTest(String name) throws Exception
	    {
	    	initSpark(name);
	    	createNetwork();
	    	train(network, 15);
	    	createTrainDataSet();
	    	createTestDataSet();
	    	test(network);
	    	dispose();
	    	log.info("***** Example Complete *****");
	    }
	    
	    private static void test(SparkDl4jMultiLayer sparkNet) throws Exception
	    {
	    	 //Perform evaluation (distributed)
	        Evaluation evaluation = sparkNet.evaluate(testData);
	        log.info("***** Evaluation *****");
	        String eval = evaluation.stats();
	        log.info(eval);
	        FileUtils.writeStringToFile(new java.io.File("C:\\tmp\\data\\spark-output\\MINST\\result.txt"), eval);	    	
	    }
	    
	    private static void createTestDataSet() throws Exception
	    {	    	
	    	  DataSetIterator iterTest = new MnistDataSetIterator(batchSizePerWorker, true, 12345);
	    	   List<DataSet> testDataList = new ArrayList<DataSet>();
	 	      
		        while (iterTest.hasNext()) {
		            testDataList.add(iterTest.next());
		        }
		        testData = sc.parallelize(testDataList);
	    }
	    
	    private static void createTrainDataSet() throws Exception
	    {
	    	 DataSetIterator iterTrain = new MnistDataSetIterator(batchSizePerWorker, true, 12345);
	    	 List<DataSet> trainDataList = new ArrayList<DataSet>();
	    	  while (iterTrain.hasNext()) {
		            trainDataList.add(iterTrain.next());
		        }
	    	  trainData = sc.parallelize(trainDataList);
	    }
	    
	    private static TrainingMaster BuildTrainingParameters()
	    {
	    	 //Configuration for Spark training: see http://deeplearning4j.org/spark for explanation of these configuration options
	        return new ParameterAveragingTrainingMaster.Builder(batchSizePerWorker)    //Each DataSet object: contains (by default) 32 examples
	            .averagingFrequency(5)
	            .workerPrefetchNumBatches(2)            //Async prefetching: 2 examples per worker
	            .batchSizePerWorker(batchSizePerWorker)
	            .build();
	    }
	    
	    //Create the Spark network
	    private static SparkDl4jMultiLayer BuildNetwork(MultiLayerConfiguration conf, TrainingMaster tm)
	    {
	    	return new SparkDl4jMultiLayer(sc, conf, tm);	    	
	    }
	    public static void dispose()
	    {
	    	  //Delete the temp training files, now that we are done with them
	        tm.deleteTempFiles(sc);	      
	    }
	    
	    public static void train(SparkDl4jMultiLayer sparkNet, int numEpochs) throws Exception {	       
	               	         	        
	        //Execute training:
	        for (int i = 0; i < numEpochs; i++) {
	            MultiLayerNetwork network = sparkNet.fit(trainData);
	           Nd4j.writeTxt(network.params(), "C:\\tmp\\data\\spark-output\\MINST\\params" + i + ".txt", ",");	          
	           FileUtils.writeStringToFile(new java.io.File("C:\\tmp\\data\\spark-output\\MINST\\conf" + i + ".json"), network.getLayerWiseConfigurations().toJson());
	            log.info("Completed Epoch {}", i);
	        }       	       
	    }
}
