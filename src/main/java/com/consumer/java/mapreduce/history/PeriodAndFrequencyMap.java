package  com.consumer.java.mapreduce.history;

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


public class PeriodAndFrequencyMap extends Mapper<Object, Text, Text, HistoryReduceInputWritable> 
{	 
	 private Text key = new Text();
	 private HistoryReduceInputWritable emitValue = new HistoryReduceInputWritable();
	 static Logger log = Logger.getLogger(PeriodAndFrequencyMap.class.getName());
	 public static final Date minDate = new Date(114, 8, 1);
	 
	 
	@Override
	public void map(Object ikey, Text value, Context context) throws IOException, InterruptedException
	{				
		String v = value.toString();
		if(v.contains("TransactionId"))
			return;
		String[] result = v.toString().split(",");
					
		//TransactionId,MemberId,TransactionAmount,TransactionDateTime,TransactionType,SessionId,Username,UserCreationDateTime,BanId,UserLastLoginDate,SiteID
		String memId = result[1];
		String transactionDate = result[3];
		SimpleDateFormat parserSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		
		int emit;	
		if(!context.getConfiguration().getBoolean("useFrequency", false))
		{
			//use money
			String amountString = result[2];
			int amount = (int)Math.round(Double.parseDouble(amountString));
			emit = amount;
		}
		else
			emit = 1;
		//2015-09-03 01:55:16.747
		Date date = null; 
								
			try {
				date = parserSDF.parse(transactionDate);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				log.error(e.toString());				
			}		
		if(date.before(minDate))
			return;
		key.set(memId);
		emitValue.setYear(date.getYear());
		emitValue.setMonth(date.getMonth() + 1);
		emitValue.setValue(emit);
	    context.write(key, emitValue);	    
	}
}
