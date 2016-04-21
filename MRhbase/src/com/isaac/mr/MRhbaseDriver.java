package com.isaac.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.fs.Path;




public class MRhbaseDriver {
	public static class MRhbaseMapper extends TableMapper<Text, IntWritable>{
		
		private IntWritable price= new IntWritable(0);
		private Text text = new Text();
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			String valEmployeeID= new String(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("EmployeeID")));
			String valPrice=new String(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("Price")));
			text.set(valEmployeeID);
			price.set(Integer.valueOf(valPrice).intValue());
			
			context.write(text, price);
		}
		
	}
	
	public static class MRhbaseReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int i=0;
			for (IntWritable val: values){
				i+=val.get();
			}
			context.write(key, new IntWritable(i));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf=HBaseConfiguration.create();
		//conf.set("hbase.regionserver.dns.nameserver","localhost");
		
		Job job=new Job(conf,"GetDataFromHbase");
		job.setJarByClass(MRhbaseDriver.class);
		
		Scan scan=new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		
		TableMapReduceUtil.initTableMapperJob(
				"Employee",
				scan,
				MRhbaseMapper.class,
				Text.class,
				IntWritable.class,
				job);
		
		job.setReducerClass(MRhbaseReducer.class);
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		
		boolean b=job.waitForCompletion(true);
		if(!b){
			throw new IOException("error with job");
		}
	}

}
