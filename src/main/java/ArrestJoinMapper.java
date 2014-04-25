import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class ArrestJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	Logger logger = Logger.getLogger(ArrestJoinMapper.class);

	private static final char OUTPUT_SEPARATOR = '\t';

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
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		boolean playerArrested = false, defensePlayerArrested = false, offensePlayerArrested = false;

		String[] pieces = value.toString().split("\\t");

		boolean[] arrests = checkArrests(pieces, pieces[4]);
		offensePlayerArrested = arrests[0];
		playerArrested = arrests[1];

		arrests = checkArrests(pieces, pieces[5]);
		defensePlayerArrested = arrests[0];
		playerArrested = playerArrested || arrests[1];

		StringBuilder output = new StringBuilder();
		output.append(playerArrested).append(OUTPUT_SEPARATOR);
		output.append(defensePlayerArrested).append(OUTPUT_SEPARATOR);
		output.append(offensePlayerArrested).append(OUTPUT_SEPARATOR);
		
		boolean homeTeamPlayerArrested = false, awayTeamPlayerArrest = false;
		
		if (pieces[4].equals(pieces[22])) {
			// Offense is home team
			homeTeamPlayerArrested = offensePlayerArrested;
			awayTeamPlayerArrest = defensePlayerArrested;
		} else {
			// Defense is home team
			homeTeamPlayerArrested = defensePlayerArrested;
			awayTeamPlayerArrest = offensePlayerArrested;
		}

		output.append(homeTeamPlayerArrested).append(OUTPUT_SEPARATOR);
		output.append(awayTeamPlayerArrest);

		if (pieces[0].equals("20121104_CAR@WAS")) {
			logger.info(pieces[0] + " " + homeTeamPlayerArrested + " " + awayTeamPlayerArrest + " " + playerArrested + " " + value.toString());
		}

		context.write(value, new Text(output.toString()));
	}

	/**
	 * Checks to see if the team has any arrests
	 * 
	 * @param pieces
	 * @param teamName
	 * @return A boolean array with {teamPlayerArrested, playerArrested}
	 */
	private boolean[] checkArrests(String[] pieces, String teamName) {
		boolean playerArrested = false, teamPlayerArrested = false;

		String season = pieces[12];

		// Check defense for arrests
		ArrayList<String> arrestedPlayers = teamSeasonToPlayersArrested.get(getKey(season, teamName));

		if (arrestedPlayers != null) {
			teamPlayerArrested = true;

			for (int i = 13; i < 17; i++) {
				if (pieces[i].length() == 0) {
					continue;
				}

				for (String arrestedPlayer : arrestedPlayers) {
					// See if the regular name is there
					if (pieces[i].equals(arrestedPlayer)) {
						playerArrested = true;

						break;
					}

					// Try it again with the initial
					String firstInitial = arrestedPlayer.substring(0, 1) + "."
							+ arrestedPlayer.substring(arrestedPlayer.indexOf(" "));

					if (firstInitial.equals(pieces[i])) {
						playerArrested = true;

						break;
					}

					// Try one more time in the play description in case it wasn't parsed
					if (pieces[9].indexOf(firstInitial) != -1 || pieces[9].indexOf(arrestedPlayer) != -1) {
						playerArrested = true;

						break;
					}
				}

				if (playerArrested == true) {
					break;
				}
			}
		}

		return new boolean[] { teamPlayerArrested, playerArrested };
	}

	private String getKey(String season, String team) {
		return season + "-" + team;
	}
}