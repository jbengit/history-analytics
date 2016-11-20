package com.consumer.java.mapreduce;

import java.io.File;

import org.apache.commons.collections.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import  com.consumer.java.mapreduce.history.*;
import  com.consumer.java.mapreduce.rfm.*;


public class MapReduceDriver {

	public class JobConfig
	{			
		public String JobName;
		
		public Class<? extends Mapper> Mapper;
		public Class<? extends Reducer> Reducer;
		public Class<?> OutputKey;
		public Class<?> OutputValue;
		
		public String InputDirectory;
		
		public KeyValue[] options = null;
	}
	
	Configuration conf = null;
	Job job = null;
	JobConfig jobConfig = null;
	
	String rootBasePath = "C:\\tmp\\data\\";
	
	public static void main(String[] args) throws Exception {
		
		MapReduceDriver instance = new MapReduceDriver();
		JobConfig conf = instance.createRFMJobConfig();
	//	JobConfig conf = instance.createHistoryByAmountJobConfig();
		//JobConfig conf = instance.createHistoryByFrequencyJobConfig();
		instance.Init(conf);
		instance.Launch();		
	}
	
	public JobConfig createRFMJobConfig()
	{
		JobConfig config = new JobConfig();
		config.JobName = "RFM";
		config.Mapper = MapRFM.class;
		config.Reducer = ReduceRFM.class;
		config.OutputKey = Text.class;
		config.OutputValue = IntAndDoubleWritable.class;
		config.InputDirectory = "transaction";
		return config;
	}
	
	public JobConfig createHistoryByAmountJobConfig()
	{
		JobConfig config = new JobConfig();
		config.JobName = "histbyamount";
		config.Mapper = PeriodAndFrequencyMap.class;
		config.Reducer = PeriodAndFrequencyReduce.class;
		config.OutputKey = Text.class;
		config.OutputValue = HistoryReduceInputWritable.class;
		config.InputDirectory = "transaction";
		return config;
	}
	
	public JobConfig createHistoryByFrequencyJobConfig()
	{
		JobConfig config = new JobConfig();
		config.JobName = "histbyfreq";
		config.Mapper = PeriodAndFrequencyMap.class;
		config.Reducer = PeriodAndFrequencyReduce.class;
		config.OutputKey = Text.class;
		config.OutputValue = HistoryReduceInputWritable.class;
		config.InputDirectory = "transaction";
		config.options = new KeyValue[1];
		config.options[0] = new KeyValue() {
			
			@Override
			public Object getValue() {
				// TODO Auto-generated method stub
				return true;
			}
			
			@Override
			public Object getKey() {
				// TODO Auto-generated method stub
				return "useFrequency";
			}
		};
		return config;
	}
	
	public void ImportFilesIntoHDFS() throws Exception
	{
		Path inputPath = new Path(rootBasePath + "\\input\\" + jobConfig.InputDirectory);
		Path outputPath = new Path("in_" + jobConfig.JobName);
		FileSystem fs = FileSystem.get(conf);
		if(!fs.exists(outputPath))
		{				
			fs.copyFromLocalFile(inputPath, outputPath);	
		}
		fs.delete(new Path("out_" + jobConfig.JobName));
	}
	
	public void ExportFilesFromHDFS() throws Exception
	{		
		Path inputPath = new Path("out_" + jobConfig.JobName);
		String outputPath = rootBasePath + "\\output\\" + jobConfig.JobName;
		FileSystem fs = FileSystem.get(conf);
		org.apache.commons.io.FileUtils.deleteDirectory(new File(outputPath));						 
		fs.copyToLocalFile(inputPath, new Path(outputPath));			
	}
	
	private void InitOptions()
	{
		if(jobConfig.options == null)
			return;
		 for (KeyValue val : jobConfig.options) 
	     {
			 conf.setBoolean((String)val.getKey(), (Boolean)val.getValue());
	     }		
	}
	
	public void Init(JobConfig config) throws Exception
	{
		jobConfig = config;
		conf = new Configuration();
		
		conf.addResource(new Path("C:\\cygwin64\\home\\joelb\\hadoop-2.6.4\\etc\\hadoop\\core-site.xml"));
		conf.addResource(new Path("C:\\cygwin64\\home\\joelb\\hadoop-2.6.4\\etc\\hadoop\\hdfs-site.xml"));
		
		InitOptions();
		
		job = Job.getInstance(conf, config.JobName);
		job.setJarByClass(MapReduceDriver.class);
		job.setMapperClass(config.Mapper);

		job.setReducerClass(config.Reducer);
		job.setOutputKeyClass(config.OutputKey);
		job.setOutputValueClass(config.OutputValue);
		ImportFilesIntoHDFS();
		FileInputFormat.setInputPaths(job, new Path("in_" + jobConfig.JobName));
		FileOutputFormat.setOutputPath(job, new Path("out_" + jobConfig.JobName));				
	}
	
	public void Launch() throws Exception
	{
		if(job.waitForCompletion(true))
		{
			ExportFilesFromHDFS();
		}
		return;					
	}

}
