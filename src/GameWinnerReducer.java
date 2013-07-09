import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class GameWinnerReducer extends Reducer<Text, Text, Text, Text> {
	Logger logger = Logger.getLogger(GameWinnerReducer.class);
	
	/** (14:56) E.Manning pass incomplete deep left to H.Nicks. */
	Pattern lastPlay = Pattern.compile("\\t([4|5])\\t(-?\\d*)\\t(\\d\\d)\\t");

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ArrayList<Text> allValues = new ArrayList<Text>();
		
		// Find the last play of the game to see who wins
		// Note: this could be done with a secondary sort and not have to cache the values
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

		Text winner;
		String[] pieces = currentEnd.split("\\t", -1);

		int offenseScore = Integer.parseInt(pieces[10].trim());
		int defenseScore = Integer.parseInt(pieces[11].trim());

		if (offenseScore == defenseScore) {
			// Last play of the game won
			winner = new Text(pieces[4]);
		} else if (offenseScore > defenseScore) {
			// Offense won the game
			winner = new Text(pieces[4]);
		} else {
			// Defense won the game
			winner = new Text(pieces[5]);
		}
		
		for (Text value : allValues) {
			context.write(value, winner);
		}
	}
}