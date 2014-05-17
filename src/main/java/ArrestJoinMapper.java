import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.jesseanderson.data.Play;

public class ArrestJoinMapper extends Mapper<Play, Text, Play, Void> {
	Logger logger = Logger.getLogger(ArrestJoinMapper.class);

	/** Maps a team by season to the players who were arrested that season */
	HashMap<String, ArrayList<String>> teamSeasonToPlayersArrested = new HashMap<String, ArrayList<String>>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// Create hash map for Map-side join
		FileSystem fileSystem = FileSystem.get(context.getConfiguration());

		FSDataInputStream dataInputStream = fileSystem.open(new Path(context.getConfiguration().get("arrestfile",
				"arrests.csv")));

		String line;

		while ((line = dataInputStream.readLine()) != null) {
			String[] pieces = line.split(",");

			String key = getKey(pieces[0], pieces[1]);

			ArrayList<String> arrestsPerSeasonAndTeam = teamSeasonToPlayersArrested.get(key);

			if (arrestsPerSeasonAndTeam == null) {
				arrestsPerSeasonAndTeam = new ArrayList<String>();
				teamSeasonToPlayersArrested.put(key, arrestsPerSeasonAndTeam);
			}

			arrestsPerSeasonAndTeam.add(pieces[2]);
		}

		dataInputStream.close();
	}

	@Override
	public void map(Play play, Text value, Context context) throws IOException, InterruptedException {
		boolean playerArrested = false, defensePlayerArrested = false, offensePlayerArrested = false;

		boolean[] arrests = checkArrests(play, true);
		offensePlayerArrested = arrests[0];
		playerArrested = arrests[1];

		arrests = checkArrests(play, false);
		defensePlayerArrested = arrests[0];
		playerArrested = playerArrested || arrests[1];

		play.setPlayerArrested(playerArrested);
		play.setDefensePlayerArrested(defensePlayerArrested);
		play.setOffensePlayerArrested(offensePlayerArrested);
		
		boolean homeTeamPlayerArrested = false, awayTeamPlayerArrest = false;
		
		if (play.getOffense().equals(play.getHomeTeam())) {
			// Offense is home team
			homeTeamPlayerArrested = offensePlayerArrested;
			awayTeamPlayerArrest = defensePlayerArrested;
		} else {
			// Defense is home team
			homeTeamPlayerArrested = defensePlayerArrested;
			awayTeamPlayerArrest = offensePlayerArrested;
		}

		play.setHomeTeamPlayerArrested(homeTeamPlayerArrested);
		play.setAwayTeamPlayerArrested(awayTeamPlayerArrest);

		context.write(play, null);
	}

	/**
	 * Checks to see if the team has any arrests
	 * 
	 * @param play
	 * @param isOffense Check the offense or the defense
	 * @return A boolean array with {teamPlayerArrested, playerArrested}
	 */
	private boolean[] checkArrests(Play play, boolean isOffense) {
		boolean playerArrested = false, teamPlayerArrested = false;

		String season = play.getYear().toString();

		// Check defense for arrests
		ArrayList<String> arrestedPlayers = teamSeasonToPlayersArrested.get(getKey(season, isOffense ? play.getOffense().toString() : play.getDefense().toString()));

		String playDesc = play.getPlayDesc().toString();
		
		if (arrestedPlayers != null) {
			teamPlayerArrested = true;

			for (String arrestedPlayer : arrestedPlayers) {
				// See if the regular name is there
				playerArrested = checkPlayers(play, isOffense, playerArrested, arrestedPlayer);
				
				if (playerArrested == true) {
					break;
				}

				// Try it again with the initial
				String firstInitial = arrestedPlayer.substring(0, 1) + "."
						+ arrestedPlayer.substring(arrestedPlayer.indexOf(" "));
				
				playerArrested = checkPlayers(play, isOffense, playerArrested, firstInitial);

				if (playerArrested == true) {
					break;
				}

				// Try one more time in the play description in case it wasn't parsed
				if (playDesc.indexOf(firstInitial) != -1 || playDesc.indexOf(arrestedPlayer) != -1) {
					playerArrested = true;

					break;
				}
			}
		}

		return new boolean[] { teamPlayerArrested, playerArrested };
	}

	private boolean checkPlayers(Play play, boolean isOffense, boolean playerArrested, String arrestedPlayer) {
		if (isOffense == true) {
			if (arrestedPlayer.equals(play.getQB()) || arrestedPlayer.equals(play.getOffensivePlayer())) {
				playerArrested = true;
			}
		} else {
			if (arrestedPlayer.equals(play.getDefensivePlayer1()) || arrestedPlayer.equals(play.getDefensivePlayer2())) {
				playerArrested = true;
			}
		}
		return playerArrested;
	}

	private String getKey(String season, String team) {
		return season + "-" + team;
	}
}