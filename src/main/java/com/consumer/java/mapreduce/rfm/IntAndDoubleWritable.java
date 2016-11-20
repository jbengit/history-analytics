package  com.consumer.java.mapreduce.rfm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntAndDoubleWritable implements Writable {

	private IntWritable i;
	private DoubleWritable d;
	
	public IntAndDoubleWritable()
	{
		this(0, 0);
	}
	
	public IntAndDoubleWritable(int v1, double v2) 
	{
		i = new IntWritable(v1);
		d = new DoubleWritable(v2);
	}
	
	public void setInt(int v)
	{
		i.set(v);		
	}
	
	public void setDouble(double v)
	{
		d.set(v);		
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		i.readFields(in);
		d.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		i.write(out);
		d.write(out);
	}
	
	public int getInt()
	{
		return i.get();
	}
	
	public double getDouble()
	{
		return d.get();
	}	

}
