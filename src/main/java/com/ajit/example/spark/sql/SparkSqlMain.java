package com.ajit.example.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkSqlMain {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkSql");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		
	}

}
