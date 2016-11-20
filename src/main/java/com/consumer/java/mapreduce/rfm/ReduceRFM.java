package com.consumer.java.mapreduce.rfm;

import java.io.IOException;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceRFM extends Reducer<Text,IntAndDoubleWritable, Text, RFMWritable> 
{		
	@Override
	public void reduce(Text key, Iterable<IntAndDoubleWritable> values, Context context) throws IOException, InterruptedException 
	{
	  int r = Integer.MAX_VALUE;
	  int f = 0;
	  double m = 0; 
      for (IntAndDoubleWritable val : values) 
      {
        f += 1;
        r = Math.min(r, val.getInt());
        //m = Math.max(m, val.getDouble());
        m += val.getDouble();
      }      
      context.write(key, new RFMWritable(r, f, (int)Math.round(m)));          
	}
}
