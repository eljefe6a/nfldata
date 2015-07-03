package solution;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class PlayAnalyzer {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

		JavaSparkContext sc = new JavaSparkContext("local", "JavaAPISuite", conf);

		SQLContext sqlContext = new SQLContext(sc);
		// Creates a DataFrame from a specified file
		DataFrame plays = sqlContext.load("output", "com.databricks.spark.avro");

		// Apply the schema to the RDD.
		sqlContext.registerDataFrameAsTable(plays, "playbyplay");

		// Run the query
		DataFrame join = sqlContext
						.sql("select playtype, pertotalstable.totalperplay, totalstable.total, ((pertotalstable.totalperplay / totalstable.total) * 100) as percentage from " +  
				"(select playtype, count(*) as totalperplay from playbyplay where rooftype <> \"None\" and prcp <= 0 group by playtype) pertotalstable " + 
				"full outer join " +
				"(select count(*) as total from playbyplay where rooftype <> \"None\" and prcp <= 0) totalstable " + 
				"order by playtype");
		
		// Output the query's rows
		join.javaRDD().collect().forEach((Row row) -> {
			System.out.println("Result:" + row.toString());
		});

	}

}
