package com.consumer.java.datasets.history;

import org.apache.commons.io.FileUtils;
import org.deeplearning4j.datasets.fetchers.BaseDataFetcher;
import org.deeplearning4j.util.MathUtils;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class HistoryDataFetcher extends BaseDataFetcher {

		public static final int NUM_EXAMPLES = 60000;
	    public static final int NUM_EXAMPLES_TEST = 10000;
	    protected static final String TEMP_ROOT = System.getProperty("user.home");
	    protected static final String MNIST_ROOT = TEMP_ROOT + File.separator + "MNIST" + File.separator;

	    protected transient HistoryManager man;
	    protected boolean binarize = true;
	    protected boolean train;
	    protected int[] order;
	    protected Random rng;
	    protected boolean shuffle;


	    /**
	     * Constructor telling whether to binarize the dataset or not
	     * @param binarize whether to binarize the dataset or not
	     * @throws IOException
	     */
	    public HistoryDataFetcher(boolean binarize) throws IOException {
	        this(binarize,true,true,System.currentTimeMillis());
	    }

	    public HistoryDataFetcher(boolean binarize, boolean train, boolean shuffle, long rngSeed) throws IOException {
	        if(!mnistExists()) {
	            new HistoryFetcher().downloadAndUntar();
	        }
	        String images;
	        String labels;
	        if(train){
	            images = MNIST_ROOT + HistoryFetcher.trainingFilesFilename_unzipped;
	            labels = MNIST_ROOT + HistoryFetcher.trainingFileLabelsFilename_unzipped;
	            totalExamples = NUM_EXAMPLES;
	        } else {
	            images = MNIST_ROOT + HistoryFetcher.testFilesFilename_unzipped;
	            labels = MNIST_ROOT + HistoryFetcher.testFileLabelsFilename_unzipped;
	            totalExamples = NUM_EXAMPLES_TEST;
	        }

	        try {
	            man = new HistoryManager(images, labels, train);
	        }catch(Exception e) {
	            FileUtils.deleteDirectory(new File(MNIST_ROOT));
	            new HistoryFetcher().downloadAndUntar();
	            man = new HistoryManager(images, labels, train);
	        }

	        numOutcomes = 10;
	        this.binarize = binarize;
	        cursor = 0;
	        inputColumns = man.getImages().getEntryLength();
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

	    private boolean mnistExists(){
	        //Check 4 files:
	        File f = new File(MNIST_ROOT,HistoryFetcher.trainingFilesFilename_unzipped);
	        if(!f.exists()) return false;
	        f = new File(MNIST_ROOT,HistoryFetcher.trainingFileLabelsFilename_unzipped);
	        if(!f.exists()) return false;
	        f = new File(MNIST_ROOT,HistoryFetcher.testFilesFilename_unzipped);
	        if(!f.exists()) return false;
	        f = new File(MNIST_ROOT,HistoryFetcher.testFileLabelsFilename_unzipped);
	        if(!f.exists()) return false;
	        return true;
	    }

	    public HistoryDataFetcher() throws IOException {
	        this(true);
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

	            byte[] img = man.readImageUnsafe(order[cursor]);
	            int label = man.readLabel(order[cursor]);

	            float[] featureVec = new float[img.length];
	            featureData[actualExamples] = featureVec;
	            labelData[actualExamples] = new float[10];
	            labelData[actualExamples][label] = 1.0f;

	            for( int j=0; j<img.length; j++ ){
	                float v = ((int)img[j]) & 0xFF; //byte is loaded as signed -> convert to unsigned
	                if(binarize){
	                    if(v > 30.0f) featureVec[j] = 1.0f;
	                    else featureVec[j] = 0.0f;
	                } else {
	                    featureVec[j] = v/255.0f;
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
