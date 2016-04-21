package com.letsdobigdata;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class MaxTemperatureDriver {

	/*		public int run(String[] args) throws Exception {
		if(args.length !=2){
			System.err.println("Usage: MaxTemperatureDriver <input path> <output path>");
			System.exit(-1);
		}
		
	Job job = new Job();
		job.setJarByClass(MaxTemperatureDriver.class);
		job.setJobName("Max Temperature");
	*/

	
	
	public static class MaxTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
	
	private static final int MISSING=9999;
	
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
	throws IOException {
		String line=value.toString();
		String year=line.substring(15,19);
		int airTemperature;
		
		if(line.charAt(87)=='+'){
			airTemperature=Integer.parseInt(line.substring(88,92));
		} else {
			airTemperature=Integer.parseInt(line.substring(87,92));
		}
		
		String quality=line.substring(92,93);
		
		if(airTemperature != MISSING && quality.matches("[01459]")) {
			output.collect(new Text(year), new IntWritable(airTemperature));
		}
	}

}
	
	public static class MaxTemperatureReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text key, Iterator<IntWritable> values, 
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		
		int maxValue=Integer.MIN_VALUE;
		while (values.hasNext()) {
			maxValue=Math.max(maxValue, values.next().get());
		}
		output.collect(key, new IntWritable(maxValue));
	}
}
	
	public static void main(String[] args) throws Exception {

		JobConf config=new JobConf(MaxTemperatureDriver.class);
		config.setJobName("Max Temperature");
		FileInputFormat.setInputPaths(config,new Path(args[0]));
		FileOutputFormat.setOutputPath(config, new Path(args[1]));
		
		config.setMapperClass(MaxTemperatureMapper.class);
		config.setCombinerClass(MaxTemperatureReducer.class);
		config.setReducerClass(MaxTemperatureReducer.class);
		config.setOutputKeyClass(Text.class);
		config.setOutputValueClass(IntWritable.class);
		config.setInputFormat(TextInputFormat.class);
		config.setOutputFormat(TextOutputFormat.class);
		JobClient.runJob(config);
	
		/*
		MaxTemperatureDriver drive= new MaxTemperatureDriver();
		int exitCode=ToolRunner.run(drive, args);
		System.exit(exitCode); */
		
	}
}
