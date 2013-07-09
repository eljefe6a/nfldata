import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class PlayByPlayMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	Logger logger = Logger.getLogger(PlayByPlayMapper.class);
	
	private static final char OUTPUT_SEPARATOR = '\t';

	/** (14:56) E.Manning pass incomplete deep left to H.Nicks. */
	Pattern incompletePass = Pattern
			.compile("([A-Z]*\\.\\s?[A-Za-z]*)\\s*pass.*incomplete.*to ([A-Z]*\\.\\s?[A-Za-z]*)");

	/** (14:49) E.Manning pass short middle to V.Cruz to NYG 21 for 5 yards (S.Lee) [J.Hatcher]. */
	Pattern completePass = Pattern
			.compile("([A-Z]*\\.\\s?[A-Za-z]*)\\s*pass.*to ([A-Z]*\\.\\s?[A-Za-z]*).*\\(?([A-Z]*\\.\\s?[A-Za-z]*)?\\)?\\s?\\[?([A-Z]*\\.\\s?[A-Za-z]*)?\\]?");

	/** (13:58) S.Weatherford punts 56 yards to DAL 23 Center-Z.DeOssie. D.Bryant to DAL 24 for 1 yard (Z.DeOssie). */
	Pattern punt = Pattern.compile("([A-Z]*\\.\\s?[A-Za-z]*)\\s*punts.*to.*\\.\\s?([A-Z]*\\.\\s?[A-Za-z]*)?");

	/** (13:44) D.Murray left guard to DAL 27 for 3 yards (C.Blackburn). */
	Pattern run = Pattern
			.compile("([A-Z]*\\.\\s?[A-Za-z]*)\\s*.*to.*\\(([A-Z]*\\.\\s?[A-Za-z]*)\\)?\\s?\\[?([A-Z]*\\.\\s?[A-Za-z]*)?\\]?");

	/** D.Bailey kicks 69 yards from DAL 35 to NYG -4. D.Wilson to NYG 16 for 20 yards (A.Holmes). */
	Pattern kickoff = Pattern
			.compile("([A-Z]*\\.\\s?[A-Za-z]*)\\s*kicks.*from.*\\.?\\s?([A-Z]*\\.\\s?[A-Za-z]*)?");

	/** (:17) (No Huddle) M.Stafford spiked the ball to stop the clock. */
	Pattern spike = Pattern.compile("([A-Z]*\\.\\s?[A-Za-z]*)\\s*spiked the ball");

	/** (9:14) L.Tynes 22 yard field goal is GOOD Center-Z.DeOssie Holder-S.Weatherford. */
	Pattern fieldGoal = Pattern.compile("([A-Z]*\\.\\s?[A-Za-z]*).*yard field goal");

	/** D.Bailey extra point is GOOD Center-L.Ladouceur Holder-C.Jones. */
	Pattern extraPoint = Pattern.compile("([A-Z]*\\.\\s?[A-Za-z]*).*extra point");

	/** (9:36) PENALTY on NYG-V.Cruz False Start 5 yards enforced at DAL 47 - No Play. */
	Pattern penalty = Pattern.compile(".*PENALTY.*");
	
	/** 20120909_STL@DET */
	Pattern gameString = Pattern.compile("(\\d*)_([A-Z]*)@([A-Z]*)");
	
	/** (3:42) J.Flacco sacked at BLT 15 for -5 yards (T.Hali). */
	Pattern sack = Pattern.compile("([A-Z]*\\.\\s?[A-Za-z]*)\\s*.*sacked.*\\(([A-Z]*\\.\\s?[A-Za-z]*)\\)?\\s?\\[?([A-Z]*\\.\\s?[A-Za-z]*)?\\]?");
	
	/** (1:18) J.Flacco kneels to BLT 40 for -1 yards. */
	Pattern kneel = Pattern.compile("([A-Z]*\\.\\s?[A-Za-z]*)\\s*kneels");

	/**
	 * (12:19) (Shotgun) R.Tannehill FUMBLES (Aborted) at MIA 49 recovered by MIA-D.Thomas at HST 49. D.Thomas to HST 49
	 * for no gain (B.Cushing).
	 */
	Pattern fumble = Pattern.compile(".*FUMBLES.*");

	Pattern[] allPatterns = { incompletePass, completePass, punt, run, kickoff, spike, fieldGoal, extraPoint, penalty,
			fumble, sack, kneel };

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		String[] pieces = line.split(",");

		String qb = "", offensivePlayer = "", defensivePlayer1 = "", defensivePlayer2 = "";
		boolean hasPenalty = false, hasFumble = false, hasIncomplete = false, isGoalGood = false;
		String playType = "";
		
		boolean found = false;

		for (Pattern pattern : allPatterns) {
			Matcher matcher = pattern.matcher(pieces[11]);

			if (matcher.find()) {
				found = true;
				
				if (pattern == incompletePass) {
					qb = matcher.group(1);
					offensivePlayer = matcher.group(2);
					hasIncomplete = true;
					playType = "PASS";
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
				} else if (pattern == run) {
					offensivePlayer = matcher.group(1);
					defensivePlayer1 = matcher.group(2);
					defensivePlayer2 = matcher.group(3);
					
					// Workaround regex bug
					if (defensivePlayer2 != null && defensivePlayer2.equals(".")) {
						defensivePlayer2 = "";
					}
					
					playType = "RUN";
				} else if (pattern == kickoff) {
					offensivePlayer = matcher.group(1);
					defensivePlayer1 = matcher.group(2);
					playType = "KICKOFF";
				} else if (pattern == spike) {
					qb = matcher.group(1);
					playType = "SPIKE";
				} else if (pattern == fieldGoal) {
					qb = matcher.group(1);
					isGoalGood = pieces[11].toLowerCase().indexOf("No Good") != -1;
					playType = "FIELDGOAL";
				} else if (pattern == extraPoint) {
					qb = matcher.group(1);
					isGoalGood = pieces[11].toLowerCase().indexOf("No Good") != -1;
					playType = "EXTRAPOINT";
				} else if (pattern == penalty) {
					hasPenalty = true;
					playType = "PENALTY";
				} else if (pattern == fumble) {
					hasFumble = true;
					playType = "FUMBLE";
				} else if (pattern == sack) {
					offensivePlayer = matcher.group(1);
					defensivePlayer1 = matcher.group(2);
					defensivePlayer2 = matcher.group(3);
					
					// Workaround regex bug
					if (defensivePlayer2 != null && defensivePlayer2.equals(".")) {
						defensivePlayer2 = "";
					}
					
					playType = "SACK";
				} else if (pattern == kneel) {
					qb = matcher.group(1);
					playType = "KNEEL";
				}

				break;
			}
		}

		if (found == false) {
			context.getCounter("inc", "notfound").increment(1);
			logger.warn("Did not match \"" + line + "\"");
			return;
		}
		
		StringBuilder output = new StringBuilder();
		
		// Add all of the pieces
		for (String piece : pieces) {
			output.append(piece).append(OUTPUT_SEPARATOR);
		}
		
		// Process the play by play data
		output.append(qb).append(OUTPUT_SEPARATOR);
		output.append(offensivePlayer).append(OUTPUT_SEPARATOR);
		output.append(defensivePlayer1).append(OUTPUT_SEPARATOR);
		output.append(defensivePlayer2).append(OUTPUT_SEPARATOR);
		output.append(hasPenalty).append(OUTPUT_SEPARATOR);
		output.append(hasFumble).append(OUTPUT_SEPARATOR);
		output.append(hasIncomplete).append(OUTPUT_SEPARATOR);
		output.append(isGoalGood).append(OUTPUT_SEPARATOR);
		output.append(playType).append(OUTPUT_SEPARATOR);
		
		// Process the game output
		Matcher gameMatcher = gameString.matcher(pieces[0]);
		
		if (gameMatcher.find()) {
			output.append(gameMatcher.group(3)).append(OUTPUT_SEPARATOR);
			output.append(gameMatcher.group(2)).append(OUTPUT_SEPARATOR);
			output.append(gameMatcher.group(1)).append(OUTPUT_SEPARATOR);
		} else {
			context.getCounter("inc", "gamenotfound").increment(1);
			logger.warn("Game did not match \"" + line + "\"");
			return;
		}
		
		// Process the game winner
		if (pieces[14] == "1") {
			// Current offense won
			output.append(pieces[4]).append(OUTPUT_SEPARATOR);
		} else {
			// Current defence won
			output.append(pieces[5]).append(OUTPUT_SEPARATOR);
		}
		
		context.write(new Text(output.toString()), NullWritable.get());
	}
}