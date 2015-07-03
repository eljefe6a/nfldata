package solution;

import com.databricks.spark.avro.AvroRelation;
import com.databricks.spark.avro.AvroSaver;
import com.databricks.spark.avro.DefaultSource;
import com.databricks.spark.avro.SchemaConverters;
import model.*;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PlayByPlay {

	private static Logger logger = Logger.getLogger(PlayByPlay.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

		JavaSparkContext sc = new JavaSparkContext("local", "JavaAPISuite", conf);

		final Broadcast<HashMap<String, ArrayList<String>>> teamSeasonToPlayersArrested = sc
				.broadcast(loadArrests("../arrests.csv", sc));

		JavaPairRDD<String, String> lines = sc.wholeTextFiles("../input/2013_nfl_pbp_data_through_wk_4.csv");
		JavaRDD<PlayData> plays = lines.flatMap((Tuple2<String, String> pair) -> {
			PlayByPlayParser playByPlayParser = new PlayByPlayParser();
			return playByPlayParser.parsePlayFile(pair._2(), pair._1());
		}).map((Play p) -> ArrestParser.parseArrest(p, teamSeasonToPlayersArrested.getValue()));

		SQLContext sqlContext = new SQLContext(sc);

		// Create the playbyplay table
		createPlayByPlay(plays, sqlContext);

		// Create the stadium table
		createStadiums(sc, sqlContext);

		// Create the weather table
		createWeather(sc, sqlContext);

		// Join all four datasets
		DataFrame join = sqlContext.sql("select *, "
				+ "(wv07 > 0 OR wv01 > 0 OR wv20 > 0 OR wv03 > 0) as hasWeatherInVicinity, "
				+ "(wt09 > 0 OR wt14 > 0 OR wt07 > 0 OR wt01 > 0 OR wt15 > 0 OR wt17 > 0 OR "
				+ "wt06 > 0 OR wt21 > 0 OR wt05 > 0 OR  wt02 > 0 OR wt11 > 0 OR wt22 > 0 OR "
				+ "wt04 > 0 OR wt13 > 0 OR wt16 > 0 OR  wt08 > 0 OR wt18 > 0 OR wt03 > 0 OR "
				+ "wt10 > 0 OR wt19 > 0) as hasWeatherType, " + "(wv07 > 0 OR wv01 > 0 OR wv20 > 0 OR wv03 > 0 OR "
				+ "wt09 > 0 OR wt14 > 0 OR wt07 > 0 OR wt01 > 0 OR wt15 > 0 OR wt17 > 0 OR "
				+ "wt06 > 0 OR wt21 > 0 OR wt05 > 0 OR  wt02 > 0 OR wt11 > 0 OR wt22 > 0 OR "
				+ "wt04 > 0 OR wt13 > 0 OR wt16 > 0 OR  wt08 > 0 OR wt18 > 0 OR wt03 > 0 OR "
				+ "wt10 > 0 OR wt19 > 0) as hasWeather " + " from playbyplay "
				+ "join stadium on stadium.team = playbyplay.hometeam "
				+ "left outer join weather on stadium.weatherstation = weather.station and playbyplay.dateplayed = weather.readingdate");

		// Recreate Avro object
		//JavaRDD<PlayData> joinedPlays = join.javaRDD().map((Row row) -> {
		//	return recreateAvro(row);
		//});
		
		AvroSaver.save(join, "output");
		// TODO: Save out as Avro file

	}

	private static PlayData recreateAvro(Row row) {
		StructType joinSchema = row.schema();
		StructField[] fields = joinSchema.fields();

		PlayData playData = new PlayData();

		Play play = new Play();
		Arrest arrest = new Arrest();
		Stadium stadium = new Stadium();
		Weather weather = new Weather();

		playData.setPlay(play);
		playData.setArrest(arrest);
		playData.setStadium(stadium);
		playData.setWeather(weather);

		SpecificRecordBase currentData = play;

		// TODO: add hasweathers

		int columnNumber = 0;

		for (StructField field : fields) {
			if (field.dataType().getClass() == ByteType.class) {
				currentData.put(columnNumber, row.getByte(columnNumber));
				break;
			} else if (field.dataType().getClass() == ShortType.class) {
				currentData.put(columnNumber, row.getShort(columnNumber));
				break;
			} else if (field.dataType().getClass() == IntegerType.class) {
				currentData.put(columnNumber, row.getInt(columnNumber));
				break;
			} else if (field.dataType().getClass() == LongType.class) {
				currentData.put(columnNumber, row.getLong(columnNumber));
				break;
			} else if (field.dataType().getClass() == FloatType.class) {
				currentData.put(columnNumber, row.getFloat(columnNumber));
				break;
			} else if (field.dataType().getClass() == DoubleType.class) {
				currentData.put(columnNumber, row.getDouble(columnNumber));
				break;
			} else if (field.dataType().getClass() == DecimalType.class) {
				currentData.put(columnNumber, row.getDecimal(columnNumber));
				break;
			} else if (field.dataType().getClass() == StringType.class) {
				currentData.put(columnNumber, row.getString(columnNumber));
				break;
			} else if (field.dataType().getClass() == BooleanType.class) {
				currentData.put(columnNumber, row.getBoolean(columnNumber));
				break;
			} else if (field.dataType().getClass() == BinaryType.class) {
				throw new RuntimeException(field.dataType().getClass() + " not supported");
			} else if (field.dataType().getClass() == TimestampType.class) {
				throw new RuntimeException(field.dataType().getClass() + " not supported");
			} else if (field.dataType().getClass() == DateType.class) {
				throw new RuntimeException(field.dataType().getClass() + " not supported");
			}

			columnNumber++;

			if (columnNumber < 29) {
				currentData = play;
			} else if (columnNumber < 33) {
				currentData = arrest;
			} else if (columnNumber < 44) {
				currentData = stadium;
			} else if (columnNumber < 93) {
				currentData = weather;
			}
		}

		return playData;
	}

	private static void createWeather(JavaSparkContext sc, SQLContext sqlContext) {
		JavaRDD<Weather> weather = sc.textFile("../173328.csv").map((String line) -> {
			return WeatherParser.parseWeather(line);
		});

		StructType schema = getStructType(new Schema[] { Weather.SCHEMA$ });

		JavaRDD<Row> rowRDD = weather.map((Weather weatherRow) -> {
			return RowFactory.create(weatherRow.get(0), weatherRow.get(1), weatherRow.get(2), weatherRow.get(3),
					weatherRow.get(4), weatherRow.get(5), weatherRow.get(6), weatherRow.get(7), weatherRow.get(8),
					weatherRow.get(9), weatherRow.get(10), weatherRow.get(11), weatherRow.get(12), weatherRow.get(13),
					weatherRow.get(14), weatherRow.get(15), weatherRow.get(16), weatherRow.get(17), weatherRow.get(18),
					weatherRow.get(19), weatherRow.get(20), weatherRow.get(21), weatherRow.get(22), weatherRow.get(23),
					weatherRow.get(24), weatherRow.get(25), weatherRow.get(26), weatherRow.get(27), weatherRow.get(28),
					weatherRow.get(29), weatherRow.get(30), weatherRow.get(31), weatherRow.get(32), weatherRow.get(33),
					weatherRow.get(34), weatherRow.get(34), weatherRow.get(36), weatherRow.get(37), weatherRow.get(38),
					weatherRow.get(39), weatherRow.get(40), weatherRow.get(41), weatherRow.get(42), weatherRow.get(43),
					weatherRow.get(44), weatherRow.get(45), weatherRow.get(46), weatherRow.get(47), weatherRow.get(48));
		});

		// Apply the schema to the RDD.
		DataFrame weatherFrame = sqlContext.createDataFrame(rowRDD, schema);
		weatherFrame.registerTempTable("weather");
	}

	private static void createStadiums(JavaSparkContext sc, SQLContext sqlContext) {
		JavaRDD<Stadium> stadiums = sc.textFile("../stadiums.csv").map((String line) -> {
			return StadiumParser.parseStadium(line);
		});

		StructType schema = getStructType(new Schema[] { Stadium.SCHEMA$ });

		JavaRDD<Row> rowRDD = stadiums.map((Stadium stadiumRow) -> {
			return RowFactory.create(stadiumRow.get(0), stadiumRow.get(1), stadiumRow.get(2), stadiumRow.get(3),
					stadiumRow.get(4), stadiumRow.get(5), stadiumRow.get(6), stadiumRow.get(7), stadiumRow.get(8),
					stadiumRow.get(9), stadiumRow.get(10));
		});

		// Apply the schema to the RDD.
		DataFrame stadiumFrame = sqlContext.createDataFrame(rowRDD, schema);
		stadiumFrame.registerTempTable("stadium");
	}

	private static void createPlayByPlay(JavaRDD<PlayData> plays, SQLContext sqlContext) {
		StructType schema = getStructType(new Schema[] { Play.SCHEMA$, Arrest.SCHEMA$ });

		// Only plays and arrest exist so far
		JavaRDD<Row> rowRDD = plays.map((PlayData playData) -> {
			return RowFactory.create(playData.getPlay().get(0), playData.getPlay().get(1), playData.getPlay().get(2),
					playData.getPlay().get(3), playData.getPlay().get(4), playData.getPlay().get(5),
					playData.getPlay().get(6), playData.getPlay().get(7), playData.getPlay().get(8),
					playData.getPlay().get(9), playData.getPlay().get(10), playData.getPlay().get(11),
					playData.getPlay().get(12), playData.getPlay().get(13), playData.getPlay().get(14),
					playData.getPlay().get(15), playData.getPlay().get(16), playData.getPlay().get(17),
					playData.getPlay().get(18), playData.getPlay().get(19), playData.getPlay().get(20),
					playData.getPlay().get(21), playData.getPlay().get(22), playData.getPlay().get(23),
					playData.getPlay().get(24), playData.getPlay().get(25), playData.getPlay().get(26),
					playData.getPlay().get(27), playData.getPlay().get(28), playData.getArrest().get(0),
					playData.getArrest().get(1), playData.getArrest().get(2), playData.getArrest().get(3),
					playData.getArrest().get(4));
		});

		// Apply the schema to the RDD.
		DataFrame playsAndArrestsFrame = sqlContext.createDataFrame(rowRDD, schema);
		playsAndArrestsFrame.registerTempTable("playbyplay");
	}

	private static HashMap<String, ArrayList<String>> loadArrests(String arrestsFile, JavaSparkContext sc) {
		JavaRDD<String> input = sc.textFile(arrestsFile, 1);

		HashMap<String, ArrayList<String>> teamSeasonToPlayersArrested = new HashMap<String, ArrayList<String>>();

		input.collect().forEach((String line) -> {
			String[] pieces = line.split(",");

			String key = ArrestParser.getKey(pieces[0], pieces[1]);

			ArrayList<String> arrestsPerSeasonAndTeam = teamSeasonToPlayersArrested.get(key);

			if (arrestsPerSeasonAndTeam == null) {
				arrestsPerSeasonAndTeam = new ArrayList<String>();
				teamSeasonToPlayersArrested.put(key, arrestsPerSeasonAndTeam);
			}

			arrestsPerSeasonAndTeam.add(pieces[2]);
		});

		return teamSeasonToPlayersArrested;
	}

	private static StructType getStructType(Schema[] schemas) {
		// Workaround because Spark SQL doesn't support Avro directly
		List<StructField> fields = new ArrayList<StructField>();

		for (Schema schema : schemas) {
			for (Schema.Field field : schema.getFields()) {
				field.schema().getType();

				// Spark SQL seems to be case sensitive
				// Normalizing to all lower case
				fields.add(DataTypes.createStructField(field.name().toLowerCase(), getDataTypeForAvro(field.schema()),
						true));
			}
		}

		return DataTypes.createStructType(fields);
	}

	private static DataType getDataTypeForAvro(Schema schema) {
		DataType returnDataType = DataTypes.StringType;

		switch (schema.getType()) {
		case INT:
			returnDataType = DataTypes.IntegerType;
			break;
		case STRING:
			returnDataType = DataTypes.StringType;
			break;
		case BOOLEAN:
			returnDataType = DataTypes.BooleanType;
			break;
		case BYTES:
			returnDataType = DataTypes.ByteType;
			break;
		case DOUBLE:
			returnDataType = DataTypes.DoubleType;
			break;
		case FLOAT:
			returnDataType = DataTypes.FloatType;
			break;
		case LONG:
			returnDataType = DataTypes.LongType;
			break;
		case FIXED:
			returnDataType = DataTypes.BinaryType;
			break;
		case ENUM:
			returnDataType = DataTypes.StringType;
			break;
		}

		return returnDataType;
	}
}
