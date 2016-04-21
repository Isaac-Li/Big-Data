package com.isaac.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import com.isaac.mr.MRdictionaryDriver.MRdictionaryMap;
import com.isaac.mr.MRdictionaryDriver.MRdictionaryReduce;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.util.ArrayList;

public class MRdictionaryMRTest{
	private MapDriver <LongWritable, Text, Text, Text> mapDriver ;
	private ReduceDriver <Text, Text,Text,Text> reduceDriver;
	
@Before
public void setUp(){
	MRdictionaryMap mapper= new MRdictionaryMap();
	mapDriver=new MapDriver<LongWritable,Text,Text,Text>();
	mapDriver.setMapper(mapper);
	MRdictionaryReduce reducer= new MRdictionaryReduce();
	reduceDriver=new ReduceDriver<Text,Text,Text,Text>();
	reduceDriver.setReducer(reducer);
	
}

  @Test
public void testMapper() throws Exception{
	//mapDriver.withInput(new LongWritable(0), new Text("a	un(e): ~book=un livre. 2."));
	//mapDriver.withOutput(new Text("a"), new Text("un(e): ~book=un livre. 2."));
	
	
	mapDriver.withInput(new LongWritable(1), new Text("a	ein"));
	mapDriver.withOutput(new Text("a"), new Text("ein"));
	mapDriver.runTest();
	
}

@Test
public void testReducer() throws Exception {
	List<Text> values=new ArrayList<Text>();
	values.add(new Text("one"));
	values.add(new Text("un"));
	values.add(new Text("ein"));
	values.add(new Text("ein(e)"));
	values.add(new Text("einem"));
	reduceDriver.withInput(new Text("a"), values);
	reduceDriver.withOutput(new Text("a"), new Text("one/un/ein/ein(e)/einem"));
	reduceDriver.runTest();
}
	
}