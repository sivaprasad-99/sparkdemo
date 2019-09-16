package com.cigna.SparkCore;

import java.util.List;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkCoreRDDTransformationsDemo {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		SparkConf sparkConf = new SparkConf().setAppName("Spark Core Transformations Demo...").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> inputRDD = sparkContext.textFile("TestSample.txt");
		
		JavaRDD<String> filter = inputRDD.filter(ravi -> ravi.contains("error")); // =>
		
		JavaRDD<String> warningsRDD = inputRDD.filter(x -> x.contains("warnings"));
		JavaRDD<String> errorsRDD = inputRDD.filter(some -> some.contains("error"));
		
		JavaRDD<String> badLinesRDD = warningsRDD.union(errorsRDD);
		List<String> badLines = badLinesRDD.collect();
		
		for (String badLine : badLines) {
			System.out.println(badLine);
			
		}
		
		badLinesRDD.saveAsTextFile("BadLinesData.txt");
		
		sparkContext.close();
		
		
		
	}

}
