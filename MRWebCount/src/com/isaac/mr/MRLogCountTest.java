package com.isaac.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;
import org.junit.Before;

public class MRLogCountTest {
	private MapDriver <LongWritable, Text, Text, IntWritable> mapDriver;
	
	@Before
	public void setUp(){
		MRLogCountMapper mapper=new MRLogCountMapper();
		mapDriver=new MapDriver<LongWritable, Text,Text,IntWritable>();
		mapDriver.setMapper(mapper);
		
	}
	
	@Test
	public void testMapper() throws Exception {
		mapDriver.withInput(new LongWritable(1),new Text("111.111.111.111 - - [16/Dec/2012:05:32:50 -0500]"));
		mapDriver.withOutput(new Text("111.111.111.111 16/Dec/2012:05"), new IntWritable(1));
		mapDriver.runTest();
	}
}
