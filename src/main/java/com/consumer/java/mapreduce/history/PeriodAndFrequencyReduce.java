package com.consumer.java.mapreduce.history;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class PeriodAndFrequencyReduce extends Reducer<Text, HistoryReduceInputWritable, Text, HistoryOutputWritable> 
{		
	private static final int MaxResultInMonth = 24;
	
	private static final Date maxDate = new Date(116, 8, 1);
	
	@Override	
	public void reduce(Text key, Iterable<HistoryReduceInputWritable> values, Context context) throws IOException, InterruptedException 
	{
	  int[] result = new int[MaxResultInMonth];
	  for(int i = 0; i < MaxResultInMonth; ++i)
	  {
		  result[i] = 0;
	  }		  
      for (HistoryReduceInputWritable val : values) 
      {
    	  //max 2016-)
    	  int previousMonths = (maxDate.getYear() - val.getYear()) * 12;
    	  
    	  int index = previousMonths + val.getMonth() - maxDate.getMonth();
        if(index > MaxResultInMonth || index < 0)
        	continue;
        result[index] += val.getValue();
      }      
      context.write(key, new HistoryOutputWritable(result));          
	}
}
