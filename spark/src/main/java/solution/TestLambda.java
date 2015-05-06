package solution;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.Arrays;

public class TestLambda {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext("local", "JavaAPISuite");

		JavaRDD<String> lines = sc.textFile("log.txt");
		JavaRDD<String> words =
				lines.flatMap(
						line -> Arrays.asList(line.split(" "))
				);
		JavaPairRDD<String, Integer> counts =
				words.mapToPair(
						w -> new Tuple2<String, Integer>(w, 1)
				).reduceByKey(
						(x, y) -> x + y
				);

		counts.collect().forEach(
                t -> System.out.println("Key:" + t._1() + " Value:" + t._2())
        );
	}
}
