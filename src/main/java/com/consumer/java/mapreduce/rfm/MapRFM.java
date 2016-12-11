package com.consumer.java.mapreduce.rfm;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class MapRFM extends Mapper<Object, Text, Text, IntAndDoubleWritable> 
{	 
	 private Text word = new Text();
	 static Logger log = Logger.getLogger(MapRFM.class.getName());
	
	 private static final Date maxDate = new Date(116, 8, 26);
	 
	@Override
	public void map(Object ikey, Text value, Context context) throws IOException, InterruptedException
	{				
		
		//word.set("hello");	    
		//context.write(word, one);
		String v = value.toString();
		if(v.contains("TransactionId"))
			return;
		String[] result = v.toString().split(",");
				
		
		//TransactionId,MemberId,TransactionAmount,TransactionDateTime,TransactionType,SessionId,Username,UserCreationDateTime,BanId,UserLastLoginDate,SiteID
		String memId = result[1];
		String transactionDate = result[3];
		String amountString = result[2];
		SimpleDateFormat parserSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		
		//2015-09-03 01:55:16.747
		Date date = null; 
								
			try {
				date = parserSDF.parse(transactionDate);
			} catch (ParseException e) {				
				log.error(e.toString());				
			}		
			double amount = Double.parseDouble(amountString);		
				
		int diffInMonths = (int) Math.floor(( maxDate.getTime() - date.getTime()) 
                / ((double)1000 * 60 * 60 * 24 * 30));		
		word.set(memId);		 
	    context.write(word, new IntAndDoubleWritable(diffInMonths, amount));	    
	}
}
