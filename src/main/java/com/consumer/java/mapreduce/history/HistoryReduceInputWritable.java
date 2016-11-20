package  com.consumer.java.mapreduce.history;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class HistoryReduceInputWritable implements Writable {

	private IntWritable value;
	private IntWritable year;
	private IntWritable month;
	
	public HistoryReduceInputWritable()
	{
		this(0, 0, 0);
	}
	
	public HistoryReduceInputWritable(int memId, int year, int month) 
	{
		this.value = new IntWritable(memId);
		this.year = new IntWritable(year);
		this.month = new IntWritable(month);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		value.readFields(in);
		year.readFields(in);
		month.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		value.write(out);
		year.write(out);
		month.write(out);
	}
	
	public void setMonth(int month)
	{
		this.month.set(month);
	}	
	
	public void setYear(int year)
	{
		this.year.set(year);
	}
	
	public int getValue()
	{
		return value.get();
	}
	
	public void setValue(int value)
	{
		this.value.set(value);
	}
	
	public int getMonth()
	{
		return month.get();
	}	
	
	public int getYear()
	{
		return year.get();
	}
}
