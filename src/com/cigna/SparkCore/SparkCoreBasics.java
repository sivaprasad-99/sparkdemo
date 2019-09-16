package com.cigna.SparkCore;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkCoreBasics {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		
		
//		SparkConf sparkConf = new SparkConf();
//		SparkConf spc = sparkConf.setAppName("Spark Core Demo.");
//		SparkConf conf = spc.setMaster("local[4]");
		
		SparkConf config = new SparkConf().setAppName("Spark Core Demo...").setMaster("local[4]");
		JavaSparkContext javaSparkContext = new JavaSparkContext(config);
		
		
		JavaRDD<String> readmeRDD = javaSparkContext.textFile("README.md"); 
		
		long count = readmeRDD.count();
		
		System.out.println("No of records in README file:"+ count);
		
		
		
		List<String> readmeData = readmeRDD.collect();
		
		for (String line : readmeData) {
			System.out.println(line);
		}
		
		
		JavaRDD<String> pythonLinesRDD = readmeRDD.filter(line -> line.contains("Python")); 
		
		System.out.println("no of lines contain python word:"+ pythonLinesRDD.count());
		
		
		List<String> pythonLines = pythonLinesRDD.collect();
		for (String pyline : pythonLines) {
			System.out.println(pyline);
		}
	
		
		
		
		
		javaSparkContext.stop();
		
		
		
	}

}

