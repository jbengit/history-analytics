package com.consumer.java;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;

import com.consumer.java.datasets.consumerHistory.ConsumerHistoryDataSetIterator;
import com.consumer.java.datasets.consumerHistory.ConsumerHistoryManager;

public class ConsumerHistoryDeepLearning extends HistoryDeepLearning {

	public ConsumerHistoryDeepLearning()
	{
		super();
		this.inNeuronCount = ConsumerHistoryManager.HISTORY_SIZE;
		this.outNeuronCount = 2;
	}
	
	private JavaRDD<DataSet> createConsumerDataSet(String rootBasePath, String jobParameter, boolean train) throws Exception
	{
		 
		DataSetIterator iterTrain = new ConsumerHistoryDataSetIterator(rootBasePath, jobParameter, batchSizePerWorker, true, 12345);
    	 List<DataSet> trainDataList = new ArrayList<DataSet>();
    	  while (iterTrain.hasNext()) {
	            trainDataList.add(iterTrain.next());
	        }
    	  return sc.parallelize(trainDataList);    	  
	}
	
	
	@Override	
	protected void createTrainDataSet(String rootBasePath, String jobParameter) throws Exception
	{		
		trainData = createConsumerDataSet(rootBasePath, jobParameter, true);
	}
	
	
	@Override
	protected void createTestDataSet(String rootBasePath, String jobParameter) throws Exception
	{
		testData = createConsumerDataSet(rootBasePath, jobParameter, false);		
	}	
	
}
