package com.isaac.mr;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MRdictionaryDriver extends Configured implements Tool{
	
	public static class MRdictionaryMap extends Mapper<LongWritable, Text, Text, Text>{
	
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException{
			String line = value.toString();
			if(line.charAt(0)!='#'){
				String[] word= value.toString().split("\t");
				context.write(new Text(word[0]), new Text(word[1]));
			}
		}
		
	}
	
	public static class MRdictionaryReduce extends 
		Reducer<Text,Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
			String strTranslationWord="";
			String strTemp=""; 
			for(Text value:values) {
				strTemp = strTemp + value.toString()+"/";
			}
			int iTranslationWord=strTemp.length()-1;
			strTranslationWord=strTemp.substring(0, iTranslationWord);
			
			context.write(key, new Text(strTranslationWord));
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception{
		if(args.length !=2){
			System.err.println("Usage: MRdictionaryDriver <input path> <output path>");
			System.exit(-1);
		}
		
		Configuration conf= new Configuration();
		Job job = new Job(conf, "MR-dictionary");

		
		//Specify various job-specific parameters
		job.setJarByClass(MRdictionaryDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
				
		job.setMapperClass(MRdictionaryMap.class);
		job.setReducerClass(MRdictionaryReduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Submit the job, then poll for progress until the job is complete
		return(job.waitForCompletion(true)? 0:1);
				
	}
	

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		MRdictionaryDriver driver = new MRdictionaryDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);

	}

}
