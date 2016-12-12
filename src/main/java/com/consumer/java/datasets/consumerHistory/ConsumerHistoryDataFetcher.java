package com.consumer.java.datasets.consumerHistory;

import org.apache.commons.io.FileUtils;
import org.deeplearning4j.datasets.fetchers.BaseDataFetcher;
import org.deeplearning4j.datasets.mnist.MnistManager;
import org.deeplearning4j.util.MathUtils;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class ConsumerHistoryDataFetcher extends BaseDataFetcher {

		public static final int NUM_EXAMPLES = 60000;
	    public static final int NUM_EXAMPLES_TEST = 10000;	    
	    
	    

	    protected transient ConsumerHistoryManager man;
	    protected boolean binarize = true;
	    protected boolean train;
	    protected int[] order;
	    protected Random rng;
	    protected boolean shuffle;
	    protected String rootPath;
	    protected String inputPath;
	    protected String historyType;

	    public ConsumerHistoryDataFetcher(String rootPath, String historyType, boolean binarize, boolean train, boolean shuffle, long rngSeed) throws IOException {
	        
	        this.historyType = historyType;
	    	this.rootPath = rootPath;
	    	this.inputPath = this.rootPath + File.separator + "output" + File.separator + this.historyType + File.separator  + "part-r-00000";
	    		    	
	        if(train){
	            
	            totalExamples = NUM_EXAMPLES;
	        } else {	            
	            totalExamples = NUM_EXAMPLES_TEST;
	        }
	        
	        int percentage = train ? 80: 20;
	        
	        man = new ConsumerHistoryManager(this.inputPath, train, percentage);	        
	        numOutcomes = 10;
	        this.binarize = binarize;
	        cursor = 0;
	        inputColumns = man.getHistoryLength();
	        this.train = train;
	        this.shuffle = shuffle;

	        if(train){
	            order = new int[NUM_EXAMPLES];
	        } else {
	            order = new int[NUM_EXAMPLES_TEST];
	        }
	        for( int i=0; i<order.length; i++ ) order[i] = i;
	        rng = new Random(rngSeed);
	        reset();    //Shuffle order
	    }
	  
	    @Override
	    public void fetch(int numExamples) {
	        if(!hasMore()) {
	            throw new IllegalStateException("Unable to getFromOrigin more; there are no more images");
	        }


	        float[][] featureData = new float[numExamples][0];
	        float[][] labelData = new float[numExamples][0];

	        int actualExamples = 0;
	        for( int i=0; i<numExamples; i++, cursor++ ){
	            if(!hasMore()) break;

	            if(!man.readElement(order[cursor]))
	            	return;
	            
	            int label = man.currentLabel;
	            
	            int[] history = man.currentHistory;
	            
	            

	            float[] featureVec = new float[history.length];
	            featureData[actualExamples] = featureVec;
	            labelData[actualExamples] = new float[10];
	            labelData[actualExamples][label] = 1.0f;

	            for( int j=0; j<history.length; j++ ){
	                float v = ((int)history[j]); //byte is loaded as signed -> convert to unsigned
	                if(binarize){
	                    if(v > 1.0f) featureVec[j] = 1.0f;
	                    else featureVec[j] = 0.0f;
	                } else {
	                    featureVec[j] = v;
	                }
	            }

	            actualExamples++;
	        }

	        if(actualExamples < numExamples){
	            featureData = Arrays.copyOfRange(featureData,0,actualExamples);
	            labelData = Arrays.copyOfRange(labelData,0,actualExamples);
	        }

	        INDArray features = Nd4j.create(featureData);
	        INDArray labels = Nd4j.create(labelData);
	        curr = new DataSet(features,labels);
	    }

	    @Override
	    public void reset() {
	        cursor = 0;
	        curr = null;
	        if(shuffle) MathUtils.shuffleArray(order, rng);
	    }

	    @Override
	    public DataSet next() {
	        DataSet next = super.next();
	        return next;
	    }

	
}
