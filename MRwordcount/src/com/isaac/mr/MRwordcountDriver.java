package com.isaac.mr;

import java.io.IOException;
import java.util.StringTokenizer;
import java.io.PrintWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class MRwordcountDriver extends Configured implements Tool{
	
	public static class MRwordcountMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private final static IntWritable one= new IntWritable(1);
		private Text words= new Text();
		private String pattern= "[^a-zA-Z0-9-']";
		
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException{
			String line= value.toString();
			line=line.replaceAll(pattern, " ");
			StringTokenizer strWord = new StringTokenizer(line);
			while(strWord.hasMoreTokens()){
				words.set(strWord.nextToken());
				context.write(words,one);
			}
		}
		
	}
	
	public static class MRwordcountReduce extends 
	Reducer<Text,IntWritable, Text, IntWritable>{
	
	private IntWritable totalsum=new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	throws IOException, InterruptedException{
		int sum=0;
		for(IntWritable val:values){
			sum+=val.get();
		}
		totalsum.set(sum);
		context.write(key, totalsum);
	}
	
}
	
	public static class MRwordcountMultipleOutputsReduce extends 
	Reducer<Text,IntWritable, Text, IntWritable>{
	
	private MultipleOutputs<Text,IntWritable> multipleOutputs;
	private int wordcount=0;
	
	@Override
	public void setup(Context context){
		multipleOutputs=new MultipleOutputs<Text,IntWritable>(context);
	}
	
	private IntWritable totalsum=new IntWritable();
	private IntWritable totalcount=new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	throws IOException, InterruptedException{
		int sum=0;
		for(IntWritable val:values){
			sum+=val.get();
			wordcount+=val.get();
		}
		totalsum.set(sum);
		totalcount.set(wordcount);
		multipleOutputs.write("countBySingleword",key, totalsum);
		multipleOutputs.write("totalCount", new Text("Total:"), totalcount);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
	
}
	
	public static class MRwordcountOutputFormat extends FileOutputFormat<Text, IntWritable>{
	
		@Override
		public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException{
			Path outpath=FileOutputFormat.getOutputPath(job);
			return new MRwordcountRecordWriter(job.getConfiguration(), outpath);
		}
	}
	
	public static class MRwordcountRecordWriter extends RecordWriter<Text, IntWritable> {
		private String separator="\t";
		private PrintWriter out;
		private int TotalCount=0;
		
		public MRwordcountRecordWriter(Configuration conf, Path output) throws IOException{
			FileSystem fs=output.getFileSystem(conf);
			FSDataOutputStream fsoutStream=fs.create(output);
			out= new PrintWriter(fsoutStream);
		}
		
		@Override
		public void write(Text key,IntWritable value) throws IOException, InterruptedException{
			out.println(key.toString()+separator+value.get());
			TotalCount+=value.get();
		}
		
		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException{
			out.println("-------------------------");
			out.println("Total:    "+String.valueOf(TotalCount));
			out.close();
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

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Specify various job-specific parameters
		job.setJarByClass(MRwordcountDriver.class);
		
		job.setMapperClass(MRwordcountMap.class);
		//job.setCombinerClass(MRwordcountReduce.class);
		job.setReducerClass(MRwordcountMultipleOutputsReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//job.setOutputFormatClass(MRwordcountOutputFormat.class);
	
			
		
		MultipleOutputs.addNamedOutput(job, "countBySingleword", TextOutputFormat.class,Text.class,IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "totalCount", TextOutputFormat.class, Text.class, IntWritable.class);
		
		//Submit the job, then poll for progress until the job is complete
		return(job.waitForCompletion(true)? 0:1);
				
	}
	

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		MRwordcountDriver driver = new MRwordcountDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);

	}

}
