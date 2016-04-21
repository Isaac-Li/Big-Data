package com.isaac.hive.connect;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;


public class ConnectHive {
	private static String DRIVER_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	public static void main(String[] args) throws SQLException {
		try{
			Class.forName(DRIVER_NAME);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		Connection con = DriverManager.getConnection("jdbc:hive://123.57.6.235:10000/default", "",""); 
		Statement stmt = con.createStatement();
		
		//create database jdbc_demo 
		String strDatabaseName = "jdbc_demo";
		String sql="CREATE DATABASE "+ strDatabaseName;
		System.out.println("Running: " + sql);
		stmt.execute(sql);
		
		sql="use "+ strDatabaseName;
		System.out.println("Running: " + sql);
		stmt.execute(sql);
		
		//create table sample_data
		String strTableName = "sample_data";
		sql="CREATE TABLE "+ strTableName + "(fristname STRING, lastname STRING)"
				+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\\n'";
		System.out.println("Running: " + sql);
		stmt.execute(sql);		
		
		//load data
		sql = "load data local inpath '/home/isaac/share/Hive-data/sample.txt' overwrite into table " + strTableName;
		System.out.println("Running: " + sql);
		stmt.execute(sql);	
	}
	

}
