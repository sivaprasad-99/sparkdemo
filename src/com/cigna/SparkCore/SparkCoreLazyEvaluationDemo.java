package com.cigna.SparkCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkCoreLazyEvaluationDemo {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("Sark core Lazy evaluation Demo").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		
		
		JavaRDD<String> textFileRDD = javaSparkContext.textFile("TestSample.txt"); // 1 crore 
		
//		JavaRDD<String> pythonLinesRDD = readmeRDD.filter(line -> line.contains("Python")); 		
		JavaRDD<String> errorLinesRDD = textFileRDD.filter(data -> data.contains("error")); // 200 error lines
		errorLinesRDD.cache();
		
		
		String first = errorLinesRDD.first();
		System.out.println("first error line in the TestSample File: "+first);
		
		long count = errorLinesRDD.count();
		System.out.println("No of error line in the TestSample File: "+count);
		
		
		
		
		javaSparkContext.stop();
		
		
	}

}
