package solution;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

public class AllQueryAnalyzer {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

		JavaSparkContext sc = new JavaSparkContext("local", "JavaAPISuite", conf);

		SQLContext sqlContext = new SQLContext(sc);
		// Creates a DataFrame from a specified file
		DataFrame plays = sqlContext.load("output", "com.databricks.spark.avro");
		
		// Cache plays in memory
		plays.persist(StorageLevel.MEMORY_ONLY());

		// Apply the schema to the RDD.
		sqlContext.registerDataFrameAsTable(plays, "playbyplay");

		StringBuilder query = new StringBuilder();
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader("../queries.hql"));
			
			String line = null;
			
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("set ")) {
					continue;
				} else if (line.startsWith("! echo ")) {
					String toOutput = line.split("\"")[1];
					System.out.println(toOutput);
				} else if (line.trim().endsWith(";")) {
					// Found the end of the query, execute it
					query.append(line);
					
					// Remove the trailing ; as Spark doesn't handle it
					query.deleteCharAt(query.length() - 1);
					
					runSQL(sqlContext, query.toString());
					
					// Reset the query StringBuilder
					query = new StringBuilder();
				} else {
					query.append(line);
				}
			}
			
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	private static void runSQL(SQLContext sqlContext, String query) {
		// Run the query
		DataFrame df = sqlContext.sql(query);
		
		// Output the query's rows
		df.javaRDD().collect().forEach((Row row) -> {
			System.out.println("Result:" + row.toString());
		});
	}

}
