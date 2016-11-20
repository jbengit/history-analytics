package  com.consumer.java.mapreduce.history;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class HistoryOutputWritable extends ArrayWritable
{
	public HistoryOutputWritable(int[] values)
	{
		super(IntWritable.class);
		IntWritable[] ints = new IntWritable[values.length];
		for(int i = 0; i < values.length; ++i)
		{
			ints[i] = new IntWritable(values[i]);
		}
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
