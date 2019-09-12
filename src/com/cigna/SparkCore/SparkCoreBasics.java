package com.cigna.SparkCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


public class SparkCoreBasics {

	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("Spark core demo").setMaster("local");
		
		System.out.println("spark conf object is created:"+ sparkConf);
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		
		System.out.println("spark context object is created :"+ javaSparkContext);
		
		
		javaSparkContext.stop();
		
	}

}
