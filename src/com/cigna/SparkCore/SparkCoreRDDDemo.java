package com.cigna.SparkCore;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkCoreRDDDemo {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("Spark core RDD demo").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		
		
		Integer[] intNum = {10,20,30,40,50};
		
		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(intNum));
		
		
		long count = intRDD.count(); 
		System.out.println("no of elements in intRDD is: "+count);
		
		
		javaSparkContext.close();
		
		
		}

}
