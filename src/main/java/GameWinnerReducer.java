import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.jesseanderson.data.Play;

public class GameWinnerReducer extends Reducer<Text, Play, NullWritable, Play> {
	Logger logger = Logger.getLogger(GameWinnerReducer.class);

	@Override
	public void reduce(Text key, Iterable<Play> values, Context context) throws IOException, InterruptedException {
		ArrayList<Play> allPlaysForGame = new ArrayList<Play>();

		// Find the last play of the game to see who wins
		// Note: this should be done with a secondary sort and not have to cache
		// the values
		// but more importantly because of sorting
		int currentLowMinute = 60;
		int currentLowSecond = 60;
		int currentHighestQuarter = 0;
		Play currentEndPlay = null;

		for (Play currentPlay : values) {
			allPlaysForGame.add(currentPlay);

			// Minutes in overtime are negative
			// 20120923_KC@NO 5 -9 32 KC NO 3 6 13 (6:32) R.Succop 31 yard field
			// goal is GOOD Center-T.Gafford
			// Holder-D.Colquitt. 24 24 2012 null null false false false false
			// RUN NO KC 20120923 NO
			if (currentPlay.getQuarter() >= currentHighestQuarter && currentPlay.getGameMinutes() <= currentLowMinute
					&& currentPlay.getGameSeconds() < currentLowSecond) {
				currentHighestQuarter = currentPlay.getQuarter();
				currentLowMinute = currentPlay.getGameMinutes();
				currentLowSecond = currentPlay.getGameSeconds();

				currentEndPlay = currentPlay;
			}

			continue;
		}

		if (currentEndPlay == null) {
			// Game doesn't contain the ending. Skip the game
			logger.warn("Current end is null");

			return;
		}

		int offenseScore = currentEndPlay.getOffenseScore();
		int defenseScore = currentEndPlay.getDefenseScore();

		// Desc at 9 - Home team at 22 Away team at 23
		if (currentEndPlay.getOffenseScore() == currentEndPlay.getDefenseScore()) {
			// Last play of the game won
			// Try to figure out what the last play was
			if (currentEndPlay.getPlayType().toString().indexOf("TOUCHDOWN") != -1) {
				// It was a touchdown
				offenseScore += 7;
			} else {
				// Otherwise, it was probably a field goal
				offenseScore += 3;
			}

			currentEndPlay.setWinner(currentEndPlay.getOffense());
		} else if (offenseScore > defenseScore) {
			// Offense won the game
			currentEndPlay.setWinner(currentEndPlay.getOffense());
		} else {
			// Defense won the game
			currentEndPlay.setWinner(currentEndPlay.getDefense());
		}

		// Was the home team on offense to output the winning score?
		int homeTeamScore, awayTeamScore;

		if (currentEndPlay.getHomeTeam().equals(currentEndPlay.getOffense())) {
			homeTeamScore = offenseScore;
			awayTeamScore = defenseScore;
		} else {
			homeTeamScore = defenseScore;
			awayTeamScore = offenseScore;
		}

		currentEndPlay.setHomeTeamScore(homeTeamScore);
		currentEndPlay.setAwayTeamScore(awayTeamScore);

		for (Play value : allPlaysForGame) {
			if (value != currentEndPlay)
				value.setWinner(currentEndPlay.getWinner());
			value.setHomeTeamScore(currentEndPlay.getHomeTeamScore());
			value.setAwayTeamScore(currentEndPlay.getAwayTeamScore());

			context.write(NullWritable.get(), value);
		}
	}
}
