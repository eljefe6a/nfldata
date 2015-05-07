package solution;

import model.Play;
import model.PlayTypes;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PlayByPlay {
    private static final char OUTPUT_SEPARATOR = '\t';
    /**
     * (14:56) E.Manning pass incomplete deep left to H.Nicks.
     */
    private static final Pattern incompletePass = Pattern
            .compile("([A-Za-z]*\\.?\\s?[A-Za-z]*)\\s*pass.*incomplete.*(to ([A-Za-z]*\\.?\\s?[A-Za-z]*))?");
    /**
     * (11:28) (Shotgun) J.Cutler pass short right intended for M.Forte
     * INTERCEPTED by J.Freeman at CHI 4. J.Freeman for 4 yards TOUCHDOWN.
     */
    private static final Pattern interception = Pattern
            .compile("([A-Za-z]*\\.\\s?[A-Za-z]*).*intended for.*INTERCEPTED by ([A-Za-z]*\\.?\\s?[A-Za-z]*)");
    /**
     * (14:49) E.Manning pass short middle to V.Cruz to NYG 21 for 5 yards
     * (S.Lee) [J.Hatcher].
     */
    private static final Pattern completePass = Pattern
            .compile("([A-Za-z]*\\.?\\s?[A-Za-z]*)\\s*pass.*to ([A-Z]*\\.\\s?[A-Za-z]*).*\\(?([A-Z]*\\.\\s?[A-Za-z]*)?\\)?\\s?\\[?([A-Z]*\\.\\s?[A-Za-z]*)?\\]?");
    /**
     * (13:58) S.Weatherford punts 56 yards to DAL 23 Center-Z.DeOssie. D.Bryant
     * to DAL 24 for 1 yard (Z.DeOssie).
     */
    private static final Pattern punt = Pattern
            .compile("([A-Z]*\\.\\s?[A-Za-z]*)\\s*punts.*to.*\\.\\s?([A-Z]*\\.\\s?[A-Za-z]*)?");
    /**
     * (13:44) D.Murray left guard to DAL 27 for 3 yards (C.Blackburn).
     */
    private static final Pattern run = Pattern
            .compile("([A-Za-z]*\\.?\\s?[A-Za-z]*)\\s*.*[to|for].*\\(?([A-Z]*\\.\\s?[A-Za-z]*)?\\)?\\s?\\[?([A-Z]*\\.\\s?[A-Za-z]*)?\\]?");
    /**
     * D.Bailey kicks 69 yards from DAL 35 to NYG -4. D.Wilson to NYG 16 for 20
     * yards (A.Holmes).
     */
    private static final Pattern kickoff = Pattern
            .compile("([A-Z]*\\.\\s?[A-Za-z]*)\\s*kicks.*from.*\\.?\\s?([A-Z]*\\.\\s?[A-Za-z]*)?");
    /**
     * (:17) (No Huddle) M.Stafford spiked the ball to stop the clock.
     */
    private static final Pattern spike = Pattern
            .compile("([A-Za-z]*\\.?\\s?[A-Za-z]*)\\s*spiked the ball");
    /**
     * (9:14) L.Tynes 22 yard field goal is GOOD Center-Z.DeOssie
     * Holder-S.Weatherford.
     */
    private static final Pattern fieldGoal = Pattern
            .compile("([A-Za-z]*\\.?\\s?[A-Za-z]*).*field goal");
    /**
     * D.Bailey extra point is GOOD Center-L.Ladouceur Holder-C.Jones.
     */
    private static final Pattern extraPoint = Pattern
            .compile("([A-Za-z]*\\.?\\s?[A-Za-z]*).*extra point");
    /**
     * (9:36) PENALTY on NYG-V.Cruz False Start 5 yards enforced at DAL 47 - No
     * Play.
     */
    private static final Pattern penalty = Pattern.compile(".*PENALTY.*");
    /**
     * (12:19) (Shotgun) R.Tannehill FUMBLES (Aborted) at MIA 49 recovered by
     * MIA-D.Thomas at HST 49. D.Thomas to HST 49 for no gain (B.Cushing).
     */
    private static final Pattern fumble = Pattern.compile(".*FUMBLES.*");
    /**
     * (3:42) J.Flacco sacked at BLT 15 for -5 yards (T.Hali).
     */
    private static final Pattern sack = Pattern
            .compile("([A-Za-z]*\\.?\\s?[A-Za-z]*)\\s*.*sacked.*\\(?([A-Z]*\\.\\s?[A-Za-z]*)\\)?\\s?\\[?([A-Z]*\\.\\s?[A-Za-z]*)?\\]?");
    /**
     * (1:18) J.Flacco kneels to BLT 40 for -1 yards.
     */
    private static final Pattern kneel = Pattern.compile("([A-Za-z]*\\.?\\s?[A-Za-z]*)\\s*kneels");
    /**
     * ** play under review ***
     */
    private static final Pattern review = Pattern.compile("play under review");
    /**
     * (5:42) Alex Smith scrambles right end to CLV 20 for 3 yards (J.Haden).
     */
    private static final Pattern scramble = Pattern
            .compile("([A-Za-z]*\\.?\\s?[A-Za-z]*)\\s*scrambles");
    /**
     * END QUARTER 3
     */
    private static final Pattern endQuarter = Pattern.compile("END [QUARTER|GAME]");
    /**
     * 20120909_STL@DET
     */
    private static final Pattern gameString = Pattern.compile("(\\d*)_([A-Z]*)@([A-Z]*)");
    private static final Pattern[] allPatterns = {incompletePass, interception, completePass, punt,
            kickoff, spike, fieldGoal, extraPoint, sack, kneel, review,
            scramble, endQuarter, run};
    private static Logger logger = Logger.getLogger(PlayByPlay.class);

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local", "JavaAPISuite");

        JavaPairRDD<String, String> lines = sc.wholeTextFiles("../input");
        JavaRDD<Play> plays =
                lines.flatMap(
                        (Tuple2<String, String> pair) -> {
                            ArrayList<Play> playsList = new ArrayList<Play>();

                            String idPrefix = pair._1().substring(0, pair._1().indexOf("_"));
                            ;
                            int id = 0;

                            String[] contentLines = pair._2().split("\n");

                            for (String line : contentLines) {
                                String[] pieces = line.split(",", -1);

                                if (pieces.length == 0) {
                                    // Skip lines that are only commas
                                    // ,,,,,,,,,,,,
                                    continue;
                                }

                                String qb = "", offensivePlayer = "", defensivePlayer1 = "", defensivePlayer2 = "";
                                boolean hasPenalty = false, hasFumble = false, hasIncomplete = false, isGoalGood = false;
                                String playType = "";

                                boolean found = false;

                                int piecesIndex = -1;

                                String playDesc = null;

                                // Sometimes the play description is in a different field
                                if (pieces[9].length() > 7) {
                                    playDesc = pieces[9];
                                    piecesIndex = 9;
                                } else if (pieces[11].length() > 7) {
                                    playDesc = pieces[11];
                                    piecesIndex = 11;
                                } else {
                                    logger.warn("Line is null \"" + line + "\"");
                                    continue;
                                }

                                if (pieces.length < piecesIndex + 2) {
                                    logger.warn("Line is not big enough \"" + line + "\"");
                                    continue;
                                }

                                for (Pattern pattern : allPatterns) {
                                    Matcher matcher = pattern.matcher(playDesc);

                                    if (matcher.find()) {
                                        found = true;

                                        if (pattern == incompletePass) {
                                            qb = matcher.group(1);
                                            offensivePlayer = matcher.group(3);
                                            hasIncomplete = true;
                                            playType = "PASS";
                                        } else if (pattern == interception) {
                                            qb = matcher.group(1);
                                            defensivePlayer1 = matcher.group(2);
                                            playType = "INTERCEPTION";
                                        } else if (pattern == completePass) {
                                            qb = matcher.group(1);
                                            offensivePlayer = matcher.group(2);
                                            defensivePlayer1 = matcher.group(3);
                                            defensivePlayer2 = matcher.group(4);
                                            playType = "PASS";
                                        } else if (pattern == punt) {
                                            qb = matcher.group(1);
                                            defensivePlayer1 = matcher.group(2);
                                            playType = "PUNT";
                                        } else if (pattern == kickoff) {
                                            offensivePlayer = matcher.group(1);
                                            defensivePlayer1 = matcher.group(2);
                                            playType = "KICKOFF";
                                        } else if (pattern == spike) {
                                            qb = matcher.group(1);
                                            playType = "SPIKE";
                                        } else if (pattern == fieldGoal) {
                                            qb = matcher.group(1);
                                            isGoalGood = playDesc.toLowerCase().indexOf("no good") == -1
                                                    && playDesc.toLowerCase().indexOf("missed") == -1;
                                            playType = "FIELDGOAL";
                                        } else if (pattern == extraPoint) {
                                            qb = matcher.group(1);
                                            isGoalGood = playDesc.toLowerCase().indexOf("no good") == -1
                                                    && playDesc.toLowerCase().indexOf("missed") == -1;
                                            playType = "EXTRAPOINT";
                                        } else if (pattern == sack) {
                                            offensivePlayer = matcher.group(1);
                                            defensivePlayer1 = matcher.group(2);
                                            defensivePlayer2 = matcher.group(3);

                                            // Workaround regex bug
                                            if (defensivePlayer2 != null
                                                    && defensivePlayer2.equals(".")) {
                                                defensivePlayer2 = "";
                                            }

                                            playType = "SACK";
                                        } else if (pattern == kneel) {
                                            qb = matcher.group(1);
                                            playType = "KNEEL";
                                        } else if (pattern == review) {
                                            playType = "REVIEW";
                                        } else if (pattern == scramble) {
                                            qb = matcher.group(1);
                                            playType = "SCRAMBLE";
                                        } else if (pattern == endQuarter) {
                                            playType = "END";
                                        } else if (pattern == run) {
                                            offensivePlayer = matcher.group(1);
                                            defensivePlayer1 = matcher.group(2);
                                            defensivePlayer2 = matcher.group(3);

                                            // Workaround regex bug
                                            if (defensivePlayer2 != null
                                                    && defensivePlayer2.equals(".")) {
                                                defensivePlayer2 = "";
                                            }

                                            playType = "RUN";
                                        }

                                        break;
                                    }
                                }

                                // Always check for penalties and fumbles
                                Matcher matcher = penalty.matcher(playDesc);

                                if (matcher.find()) {
                                    hasPenalty = true;
                                }

                                matcher = fumble.matcher(playDesc);

                                if (matcher.find()) {
                                    hasFumble = true;
                                }


                                if (found == false) {
                                    // TODO: Add again
                                    //context.getCounter("inc", "notfound").increment(1);
                                    //logger.warn("Did not match \"" + line + "\"");

                                    continue;
                                }

                                // Process the game output
                                Matcher gameMatcher = gameString.matcher(pieces[0]);

                                // Process the game output
                                if (gameMatcher.find()) {
                                    // Check that offense and defense is filled in
                                    if (pieces[4].trim().length() == 0) {
                                        pieces[4] = gameMatcher.group(3).equals(pieces[5]) ? gameMatcher
                                                .group(2) : gameMatcher.group(3);
                                        logger.warn("Replacing offense to be " + pieces[4] + " Off:"
                                                + pieces[5]);
                                    }

                                    if (pieces[5].trim().length() == 0) {
                                        pieces[4] = gameMatcher.group(3).equals(pieces[4]) ? gameMatcher
                                                .group(3) : gameMatcher.group(2);
                                        logger.warn("Replacing offense to be " + pieces[5] + " Def:"
                                                + pieces[4]);
                                    }
                                } else {
                                    // TODO: Add again
                                    /*
                                    context.getCounter("inc", "gamenotfound").increment(1);
                                    logger.warn("Game did not match \"" + line + "\"");
                                    */
                                    continue;
                                }

                                // Output the unique id of the play
                                String playId = idPrefix + "_" + StringUtils.leftPad(String.valueOf(id), 8, "0");
                                id++;

                                int offenseScore = 0;
                                int defenseScore = 0;
                                int year = 0;

                                // Add all of the pieces
                                for (int i = 0; i < pieces.length; i++) {
                                    // Normalize output across all seasons because some seasons have more data
                                    if (piecesIndex == 11) {
                                        offenseScore = NumberUtils.toInt(pieces[15]);
                                        defenseScore = NumberUtils.toInt(pieces[16]);
                                        year = NumberUtils.toInt(pieces[17]);
                                    } else {
                                        offenseScore = NumberUtils.toInt(pieces[9]);
                                        defenseScore = NumberUtils.toInt(pieces[10]);
                                        year = NumberUtils.toInt(pieces[11]);
                                    }
                                }

                                Play play = new Play(pieces[0], NumberUtils.toInt(pieces[1]), NumberUtils.toInt(pieces[2]), NumberUtils.toInt(pieces[3]),
                                        pieces[4], pieces[5], NumberUtils.toInt(pieces[6]), NumberUtils.toInt(pieces[7]), NumberUtils.toInt(pieces[8]),
                                        playDesc, offenseScore, defenseScore, year,
                                        qb, offensivePlayer, defensivePlayer1, defensivePlayer2,
                                        Boolean.valueOf(hasPenalty), Boolean.valueOf(hasFumble), Boolean.valueOf(hasIncomplete), Boolean.valueOf(isGoalGood),
                                        PlayTypes.valueOf(playType), gameMatcher.group(3), gameMatcher.group(2), gameMatcher.group(1), playId,
                                        "winner", 0, 0, false, false, false, false, false);

                                playsList.add(play);
                            }

                            return playsList;
                        }
                );
        /*
        public Play(java.lang.CharSequence Game, java.lang.Integer Quarter, java.lang.Integer GameMinutes, java.lang.Integer GameSeconds,
                java.lang.CharSequence Offense, java.lang.CharSequence Defense, java.lang.Integer Down, java.lang.Integer YardsToGo, java.lang.Integer YardLine,
                java.lang.CharSequence PlayDesc, java.lang.Integer OffenseScore, java.lang.Integer DefenseScore, java.lang.Integer Year,
                java.lang.CharSequence QB,
                java.lang.CharSequence OffensivePlayer, java.lang.CharSequence DefensivePlayer1, java.lang.CharSequence DefensivePlayer2,
                java.lang.Boolean Penalty, java.lang.Boolean Fumble, java.lang.Boolean Incomplete, java.lang.Boolean IsGoalGood, model.PlayTypes PlayType,
                java.lang.CharSequence HomeTeam, java.lang.CharSequence AwayTeam, java.lang.CharSequence DatePlayed, java.lang.CharSequence PlayId,
                java.lang.CharSequence Winner, java.lang.Integer HomeTeamScore, java.lang.Integer AwayTeamScore, java.lang.Boolean PlayerArrested,
                java.lang.Boolean OffensePlayerArrested, java.lang.Boolean DefensePlayerArrested, java.lang.Boolean HomeTeamPlayerArrested,
                java.lang.Boolean AwayTeamPlayerArrested) {
                */
        plays.collect().forEach(
                t -> System.out.println("Away:" + t.getPlayDesc())
                //t -> System.out.println("Away:" + t.getAwayTeam() + " Home:" + t.getHomeTeam())
        );
    }

    private static Integer safeParse(String number) {
        if (number.length() == 0) {
            return 0;
        } else {
            try {
                return 0;
            } catch (NumberFormatException e) {
                return 0;
            }
        }
    }
}
