package com.isaac.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.Text;

public class MRLogCountDriver extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception{
		
		//System.out.print(args.length);
		//System.out.print(args[0]);
		//System.out.print(args[1]);
		
		if(args.length !=2){
				System.err.println("Usage: MRLogcountPerHour <input path>  <output path>");
				System.exit(-1);
			}

	
		Job job= new Job();
		job.setJarByClass(MRLogCountPerHour.class);
		job.setJobName("Web count per hour");
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MRLogCountMapper.class);
		job.setReducerClass(MRLogCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		return(job.waitForCompletion(true)?0:1);
	}
}