package com.acadgild.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class Assgn3_Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String line = value.toString().trim();
		String[] data = line.trim().split("\\|");
		
		//Samsung|Optima|14|Madhya Pradesh|132401|14200
		
		if (data.length > 0) 
		{
			String compName = data[0];
			String prodName = data[1];
			if ((compName != null) &&
				(prodName != null) &&
				!(compName.equalsIgnoreCase("NA")) && 
				!(prodName.equalsIgnoreCase("NA")))
			{
				context.write(new Text(compName), new LongWritable(1));				
			}
		}
	}
}


