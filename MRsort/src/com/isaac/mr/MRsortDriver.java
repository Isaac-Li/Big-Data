package com.isaac.mr;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.util.Iterator;



public class MRsortDriver {
	
	public static class MRsortMap extends IdentityMapper<Text, Text>{
		
		public void map(Text key, Text value, OutputCollector<Text,Text> output, Reporter reporter)
		throws IOException{
			
			if(key.getLength()>0){
				output.collect(key, value);				
			}
		}		
	}
	
	/*
	public static class MRsortReduce extends IdentityReducer<Text, Text>{
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter)
		throws IOException{
				output.collect(key, new Text(""));
	}
		
	} */		

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		JobConf config= new JobConf(MRsortDriver.class);
		config.setJobName("Sorting");
		config.setOutputKeyClass(Text.class);
		config.setOutputValueClass(Text.class);
		config.setNumReduceTasks(1);
		config.setMapperClass(MRsortMap.class);
		config.setReducerClass(IdentityReducer.class);
		//config.setOutputKeyComparatorClass(theClass);
		config.setInputFormat(KeyValueTextInputFormat.class);
		//config.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(config, new Path(args[0]));
		FileOutputFormat.setOutputPath(config,new Path(args[1]));
		JobClient.runJob(config);

	}

}
