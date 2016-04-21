package com.isaac.mr;

import org.apache.hadoop.util.ToolRunner;


public class MRLogCountPerHour {
	public static void main(String[] args) throws Exception{
		MRLogCountDriver driver= new MRLogCountDriver();
		int exitCode=ToolRunner.run(driver, args);
		System.exit(exitCode);
			
	}

}
