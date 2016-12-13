package com.consumer.java.datasets.consumerHistory;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.consumer.java.HistoryDeepLearning;

import breeze.linalg.logAndNormalize;

public class ConsumerHistoryManager {
	    private byte[][] imagesArr;
	    private int[] labelsArr;
	    public static final int HISTORY_SIZE = 12; //  last year + 3 months.
	    private static final int TEST_PERIOD = 3; // 3 months to check if the user spent or not.
	    	    
		public int currentLabel;
		public int[] currentHistory;
		private int currentIndex;
		private List<String> historyLines;
		
		  private static final Logger log = LoggerFactory.getLogger(ConsumerHistoryManager.class);
	    
	    /**
	     * Constructs an instance managing the two given data files. Supports
	     * <code>NULL</code> value for one of the arguments in case reading only one
	     * of the files (images and labels) is required.
	     *
	     * @param imagesFile
	     *            Can be <code>NULL</code>. In that case all future operations
	     *            using that file will fail.
	     * @param labelsFile
	     *            Can be <code>NULL</code>. In that case all future operations
	     *            using that file will fail.
	     * @throws IOException
	     */
	    public ConsumerHistoryManager(String inputFilePath, boolean train, int percentage) throws IOException {
	    	loadData(inputFilePath, train, percentage);
	    }
	    
	    private void loadData(String inputFilePath, boolean fromStart, int percentage) throws IOException
	    {
 	    	log.info("**** loading data from " + inputFilePath + "...");
	    	List<String> lines= Files.readAllLines(Paths.get(inputFilePath),
	    			StandardCharsets.UTF_8);
	    	log.info("**** " + lines.size() + " loaded.");
	    	log.info("**** selecting" + percentage + " %...");
	    	int fromIndex = fromStart ? 0 :  lines.size() * percentage / 100;
	    	
	    	int toIndex = fromStart ? ((lines.size() * percentage / 100)) :  lines.size() -1;
	    	historyLines = lines.subList(fromIndex, toIndex);
	    	log.info("**** " + historyLines.size() + " selected.");
	    	currentIndex  = 0;
	    	currentHistory = new int[HISTORY_SIZE];
	    }

		public int getHistoryLength() {
			return HISTORY_SIZE;
		}
			
		private void parseHistoryLine(String line)
		{
			String[] parts = line.split("\t")[1].split(" ");
			//load the history : HISTORY_SIZE skipping TEST_PERIOD 
			for(int i = TEST_PERIOD; i < HISTORY_SIZE; ++i)
			{
				currentHistory[i - TEST_PERIOD] = Integer.parseInt(parts[i]);
			}
			//Now compute the label.
			//If at least the user spent if the 3 first month, we mark the label as suceed.
			currentLabel = 0;
			for(int i = 0; i < TEST_PERIOD; ++i)
			{
				if(Integer.parseInt(parts[i]) > 0)
				{
					currentLabel = 1;
					break;
				}
			}
		}

		public boolean readElement(int i) {
			currentIndex = i;
			if(!hasMore())
				return false;			
			parseHistoryLine(historyLines.get(i));
			return true;
		}

		public boolean hasMore() {
			return currentIndex < historyLines.size();
		}

}
