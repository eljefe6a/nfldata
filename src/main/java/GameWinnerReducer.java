import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class GameWinnerReducer extends Reducer<Text, Text, Text, Text> {
	Logger logger = Logger.getLogger(GameWinnerReducer.class);
	
	private static final char OUTPUT_SEPARATOR = '\t';
	
	/** 20120923_KC@NO 5 -9 32 KC NO 3 6 13 */
	Pattern lastPlay = Pattern.compile("\\t([4|5])\\t(-?\\d*)\\t(\\d\\d)\\t");

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ArrayList<Text> allValues = new ArrayList<Text>();
		
		// Find the last play of the game to see who wins
		// Note: this should be done with a secondary sort and not have to cache the values
		// but more importantly because of sorting
		int currentLowMinute = 60;
		int currentLowSecond = 60;
		int currentHighestQuarter = 0;
		String currentEnd = null;

		for (Text value : values) {
			allValues.add(new Text(value));
			
			// Process the game output
			String play = value.toString();

			Matcher gameMatcher = lastPlay.matcher(play);

			// Minutes in overtime are negative
			// 20120923_KC@NO 5 -9 32 KC NO 3 6 13 (6:32) R.Succop 31 yard field goal is GOOD Center-T.Gafford
			// Holder-D.Colquitt. 24 24 2012 null null false false false false RUN NO KC 20120923 NO
			if (gameMatcher.find()) {
				int quarter = Integer.parseInt(gameMatcher.group(1));
				int minutes = Integer.parseInt(gameMatcher.group(2));
				int seconds = Integer.parseInt(gameMatcher.group(3));

				if (quarter >= currentHighestQuarter && minutes <= currentLowMinute && seconds < currentLowSecond) {
					currentHighestQuarter = quarter;
					currentLowMinute = minutes;
					currentLowSecond = seconds;

					currentEnd = play;
				}

				continue;
			}
		}
		
		if (currentEnd == null) {
			// Game doesn't contain the ending.  Skip the game
			logger.warn("Current end is null");
			
			return;
		}
		
		StringBuilder output = new StringBuilder();

		String[] pieces = currentEnd.split("\\t", -1);

		int offenseScore = Integer.parseInt(pieces[10].trim());
		int defenseScore = Integer.parseInt(pieces[11].trim());
		// Desc at 9 - Home team at 22 Away team at 23
		if (offenseScore == defenseScore) {
			// Last play of the game won
			output.append(pieces[4]).append(OUTPUT_SEPARATOR);
			
			// Try to figure out what the last play was
			if (pieces[9].toUpperCase().indexOf("TOUCHDOWN") != -1) {
				// It was a touchdown
				offenseScore += 7;
			} else {
				// Otherwise, it was probably a field goal
				offenseScore += 3;
			}
		} else if (offenseScore > defenseScore) {
			// Offense won the game
			output.append(pieces[4]).append(OUTPUT_SEPARATOR);
		} else {
			// Defense won the game
			output.append(pieces[5]).append(OUTPUT_SEPARATOR);
		}
		
		// Was the home team on offense to output the winning score?
		int homeTeamScore, awayTeamScore;
		
		if (pieces[4].equals(pieces[22])) {
			homeTeamScore = offenseScore;
			awayTeamScore = defenseScore;
		} else {
			homeTeamScore = defenseScore;
			awayTeamScore = offenseScore;
		}

		output.append(homeTeamScore).append(OUTPUT_SEPARATOR);
		output.append(awayTeamScore);
		
		for (Text value : allValues) {
			context.write(value, new Text(output.toString()));
		}
	}
}