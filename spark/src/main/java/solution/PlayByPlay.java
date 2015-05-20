package solution;

import com.databricks.spark.avro.AvroRelation;
import com.databricks.spark.avro.DefaultSource;
import com.databricks.spark.avro.SchemaConverters;
import model.*;
import org.apache.avro.Schema;
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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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

        final Broadcast<HashMap<String, ArrayList<String>>> teamSeasonToPlayersArrested = sc.broadcast(loadArrests("../arrests.csv", sc));

        JavaPairRDD<String, String> lines = sc.wholeTextFiles("../input/2013_nfl_pbp_data_through_wk_4.csv");
        JavaRDD<PlayData> plays =
                lines.flatMap(
                        (Tuple2<String, String> pair) -> {
                            PlayByPlayParser playByPlayParser = new PlayByPlayParser();
                            return playByPlayParser.parsePlayFile(pair._2(), pair._1());
                        }
                ).map(
                        (Play p) -> ArrestParser.parseArrest(p, teamSeasonToPlayersArrested.getValue())
                );

        SQLContext sqlContext = new SQLContext(sc);

        // Create the playbyplay table
        //DataFrame playbyplaytable = createPlayByPlay(plays, sqlContext);

        // Create the stadium table
        //DataFrame stadiums = createStadiums(sc, sqlContext);

        // Create the weather table
        DataFrame weather = createWeather(sc, sqlContext);

        DataFrame join = sqlContext.sql("select * from weather");
        // TODO: Join and create united play data

        // TODO: Save out as Avro file

        /*
        DataFrame join = sqlContext.sql("select *, " +
                "(WV07 > 0 OR WV01 > 0 OR WV20 > 0 OR WV03 > 0) as hasWeatherInVicinity, " +
                "(WT09 > 0 OR WT14 > 0 OR WT07 > 0 OR WT01 > 0 OR WT15 > 0 OR WT17 > 0 OR " +
                "WT06 > 0 OR WT21 > 0 OR WT05 > 0 OR  WT02 > 0 OR WT11 > 0 OR WT22 > 0 OR " +
                "WT04 > 0 OR WT13 > 0 OR WT16 > 0 OR  WT08 > 0 OR WT18 > 0 OR WT03 > 0 OR " +
                "WT10 > 0 OR WT19 > 0) as hasWeatherType, " +
                "(WV07 > 0 OR WV01 > 0 OR WV20 > 0 OR WV03 > 0 OR " +
                "WT09 > 0 OR WT14 > 0 OR WT07 > 0 OR WT01 > 0 OR WT15 > 0 OR WT17 > 0 OR " +
                "WT06 > 0 OR WT21 > 0 OR WT05 > 0 OR  WT02 > 0 OR WT11 > 0 OR WT22 > 0 OR " +
                "WT04 > 0 OR WT13 > 0 OR WT16 > 0 OR  WT08 > 0 OR WT18 > 0 OR WT03 > 0 OR " +
                "WT10 > 0 OR WT19 > 0) as hasWeather " +
                " from playbyplay " +
                "join stadium on stadium.team = playbyplay.hometeam " +
                "left outer join weather on stadium.weatherstation = weather.station and playbyplay.dateplayed = weather.readingdate");
        */

        join.javaRDD().collect().forEach(
                (Row row) -> {
                    //System.out.println("Away:" + row.toString());
                }

        );

    }

    private static DataFrame createWeather(JavaSparkContext sc, SQLContext sqlContext) {
        JavaRDD<Weather> weather = sc.textFile("../173328.csv").map(
                (String line) -> {
                    return WeatherParser.parseWeather(line);
                }
        );


        // Workaround because Spark SQL doesn't support Avro directly
        List<StructField> fields = new ArrayList<StructField>();

        for (Schema.Field field : Weather.SCHEMA$.getFields()) {
            field.schema().getType();

            fields.add(DataTypes.createStructField(field.name(), getDataTypeForAvro(field.schema()), true));
        }

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = weather.map(
                (Weather weatherRow) -> {
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

        return weatherFrame;
    }

    private static DataFrame createStadiums(JavaSparkContext sc, SQLContext sqlContext) {
        JavaRDD<Stadium> stadiums = sc.textFile("../stadiums.csv").map(
                (String line) -> {
                    return StadiumParser.parseStadium(line);
                }
        );

        DataFrame stadiumsTable = sqlContext.createDataFrame(stadiums, Stadium.class);
        stadiumsTable.registerTempTable("stadium");

        System.out.println("Printing schema");
        stadiumsTable.printSchema();

        return null;
    }

    private static DataFrame createPlayByPlay(JavaRDD<PlayData> plays, SQLContext sqlContext) {
        DataFrame playbyplaytable = sqlContext.applySchema(plays, SchemaConverters.class);
        playbyplaytable.registerTempTable("playbyplay");
        return playbyplaytable;
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
