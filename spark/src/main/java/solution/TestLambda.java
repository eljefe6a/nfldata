package solution;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;

import java.util.Arrays;

public class TestLambda {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext("local", "JavaAPISuite");

		JavaRDD<String> lines = sc.textFile("log.txt", 1)
				.filter(s -> s.contains("asdf"));
		long numErrors = lines.count();

		System.out.println(numErrors);
	}
}
