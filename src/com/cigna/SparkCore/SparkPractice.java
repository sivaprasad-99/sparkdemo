package com.cigna.SparkCore;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.clearspring.analytics.stream.membership.Filter;

public class SparkPractice {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf config = new SparkConf().setAppName("Spark practice").setMaster("local");
		// configurations details 
		JavaSparkContext javaSparkContext = new JavaSparkContext(config);
		// like an entry point to the program
		
		
		
		
		
	JavaRDD<String> testRDD = javaSparkContext.textFile("TestSample.txt");
	// creating an RDD using a text file
	long count = testRDD.count();
	
	System.out.println("count of readme RDD:" + count );
	// performing the count action
	
List<String> collect = testRDD.collect();
for (String siva : collect) {
	System.out.println(siva);
	// printing all the lines in the file.
}
	JavaRDD<String> sivaRDD = testRDD.filter(line -> line.contains("error"));
	System.out.println(" no of lines contains error :" + sivaRDD.count());
	// for printing lines containing error work

	List<String> errorcollect = sivaRDD.collect();
for (String line : errorcollect) {
	System.out.println(line);

}
	
	javaSparkContext.stop();
		
	}

	}
