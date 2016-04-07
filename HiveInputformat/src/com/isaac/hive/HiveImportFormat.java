package com.isaac.hive;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;


public class HiveImportFormat extends TextInputFormat{
	
	@Override
	  public RecordReader<LongWritable, Text> getRecordReader(
	  InputSplit genericSplit, JobConf job, Reporter reporter)
	  throws IOException {
		reporter.setStatus(genericSplit.toString());
		HiveRecordReader reader = new HiveRecordReader(new LineRecordReader(job, (FileSplit) genericSplit));
		return reader;
	 }
							 
	  public static class HiveRecordReader implements RecordReader<LongWritable, Text> {
		LineRecordReader reader;
		Text text;
		public HiveRecordReader(LineRecordReader reader) {
		 this.reader = reader;
		 text = reader.createValue();
		}
		
		@Override
							 
		public void close() throws IOException {
			reader.close();
		}
							 
		@Override
		public LongWritable createKey() {
			return reader.createKey();
		}
		
		@Override
		public Text createValue() {
			return new Text();
		}
							 
		@Override
		public long getPos() throws IOException {
			return reader.getPos();
							 
		}
							 
		@Override
							 
		public float getProgress() throws IOException {
			return reader.getProgress();
		}
				
		private String pattern1="[\"]";
		private String pattern2="[,]";
		
		@Override					 
		public boolean next(LongWritable key, Text value)  throws IOException {
			while (reader.next(key, text)) {
				
				String strReplace = text.toString();
				strReplace=strReplace.replaceAll(pattern1, "");
				strReplace=strReplace.replaceAll(pattern2, "\001");
				Text txtReplace = new Text();
				txtReplace.set(strReplace);
				value.set(txtReplace.getBytes(), 0, txtReplace.getLength());
				return true;
			}
							 
		  return false;
		}
		}
}
