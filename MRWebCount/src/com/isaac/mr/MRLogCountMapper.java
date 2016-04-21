package com.isaac.mr;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class MRLogCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
		
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException{
	
		String line=value.toString();
		 
		if(line.contains("]")){
			String[] outline=value.toString().split(" ");
			String strDate=outline[3].substring(1, 15);
			String output=outline[0]+" "+strDate;
			context.write(new Text(output),new IntWritable(1));
		}
	}

}
