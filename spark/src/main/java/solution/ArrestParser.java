package solution;

import model.Arrest;
import model.Play;
import model.PlayData;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by vmuser on 5/8/15.
 */
public class ArrestParser {
    public static PlayData parseArrest(Play play, HashMap<String, ArrayList<String>> teamSeasonToPlayersArrested) {
        Arrest arrest = new Arrest();

        processPlay(play, arrest);

        return new PlayData(play, arrest, null, null);
    }

    private static void processPlay(Play play, Arrest arrest, HashMap<String, ArrayList<String>> teamSeasonToPlayersArrested) {
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
     */
    private boolean[] checkArrests(Play play, boolean checkOffense, HashMap<String, ArrayList<String>> teamSeasonToPlayersArrested) {
        boolean playerArrested = false, teamPlayerArrested = false;

        String season = play.getYear().toString();

        // Check for arrests
        ArrayList<String> arrestedPlayers = teamSeasonToPlayersArrested.get(getKey(season, checkOffense ? play.getOffense().toString() : play.getDefense().toString()));

        if (arrestedPlayers != null) {
            teamPlayerArrested = true;

            if (checkOffense) {
                playerArrested = wasPlayerArrested(play.getQB().toString(), arrestedPlayers);

                if (!playerArrested) {
                    playerArrested = wasPlayerArrested(play.getOffensivePlayer().toString(), arrestedPlayers);
                }
            } else {
                playerArrested = wasPlayerArrested(play.getDefensivePlayer1().toString(), arrestedPlayers);

                if (!playerArrested && play.getDefensivePlayer2() != null) {
                    playerArrested = wasPlayerArrested(play.getDefensivePlayer2().toString(), arrestedPlayers);
                }
            }
        }

        return new boolean[] { teamPlayerArrested, playerArrested };
    }

    private boolean wasPlayerArrested(String player, ArrayList<String> arrestedPlayers) {
        for (String arrestedPlayer : arrestedPlayers) {
            // See if the regular name is there
            if (player.equals(arrestedPlayer)) {
                return true;
            }

            // Try it again with the initial
            String firstInitial = arrestedPlayer.substring(0, 1) + "."
                    + arrestedPlayer.substring(arrestedPlayer.indexOf(" "));

            if (firstInitial.equals(player)) {
                return true;
            }

            // Try one more time in the play description in case it wasn't parsed
            if (pieces[9].indexOf(firstInitial) != -1 || pieces[9].indexOf(arrestedPlayer) != -1) {
                playerArrested = true;

                break;
            }
        }

        return false;
    }

    public static String getKey(String season, String team) {
        return season + "-" + team;
    }
}
