package com.consumer.java.mapreduce.rfm;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class RFMWritable extends ArrayWritable
{
	public RFMWritable()
	{
		super(IntWritable.class);
	}
	
	public RFMWritable(int r, int f, int m)
	{
		super(IntWritable.class);
		IntWritable[] ints = new IntWritable[3];
		ints[0] = new IntWritable(r);
		ints[1] = new IntWritable(f);
		ints[2] = new IntWritable(m);
		set(ints);
	}
	
	 @Override
	    public String toString() {
	        StringBuilder sb = new StringBuilder();
	        for (String s : super.toStrings())
	        {
	            sb.append(s).append(" ");
	        }
	        return sb.toString();
	    }
}
