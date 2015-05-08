package solution;

import model.Play;
import model.PlayTypes;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class TestAvro {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaSparkContext sc = new JavaSparkContext("local", "JavaAPISuite", conf);

        JavaRDD<String> lines = sc.textFile("log.txt");
        JavaRDD<Play> plays =
                lines.map(
                        line -> new Play("game", 1, 1, 1, "offense", "defense", 1, 10, 20, "play desc", 0, 0, 2000, "qb", "offensiveplayer", "defense1", "defense2",
                                false, false, false, false, PlayTypes.PASS, "home", "away", "dateplayed", "playid", "winner", 0, 0)
                );
        /*
        public Play(java.lang.CharSequence Game, java.lang.Integer Quarter, java.lang.Integer GameMinutes, java.lang.Integer GameSeconds,
                java.lang.CharSequence Offense, java.lang.CharSequence Defense, java.lang.Integer Down, java.lang.Integer YardsToGo, java.lang.Integer YardLine,
                java.lang.CharSequence PlayDesc, java.lang.Integer OffenseScore, java.lang.Integer DefenseScore, java.lang.Integer Year, java.lang.CharSequence QB,
                java.lang.CharSequence OffensivePlayer, java.lang.CharSequence DefensivePlayer1, java.lang.CharSequence DefensivePlayer2,
                java.lang.Boolean Penalty, java.lang.Boolean Fumble, java.lang.Boolean Incomplete, java.lang.Boolean IsGoalGood, model.PlayTypes PlayType,
                java.lang.CharSequence HomeTeam, java.lang.CharSequence AwayTeam, java.lang.CharSequence DatePlayed, java.lang.CharSequence PlayId,
                java.lang.CharSequence Winner, java.lang.Integer HomeTeamScore, java.lang.Integer AwayTeamScore, java.lang.Boolean PlayerArrested,
                java.lang.Boolean OffensePlayerArrested, java.lang.Boolean DefensePlayerArrested, java.lang.Boolean HomeTeamPlayerArrested,
                java.lang.Boolean AwayTeamPlayerArrested) {
                */
        plays.collect().forEach(
                    t -> System.out.println("Away:" + t.getDefense())
                    //t -> System.out.println("Away:" + t.getAwayTeam() + " Home:" + t.getHomeTeam())
            );
        }

    }
