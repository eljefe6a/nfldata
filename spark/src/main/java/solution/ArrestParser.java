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

        processPlay(play, arrest, teamSeasonToPlayersArrested);

        return new PlayData(play, arrest, null, null);
    }

    private static void processPlay(Play play, Arrest arrest, HashMap<String, ArrayList<String>> teamSeasonToPlayersArrested) {
        checkArrests(play, arrest, true, teamSeasonToPlayersArrested);

        checkArrests(play, arrest, false, teamSeasonToPlayersArrested);

        if (play.getOffense().equals(play.getHomeTeam())) {
            // Offense is home team
            arrest.setHomeTeamPlayerArrested(arrest.getOffensePlayerArrested());
            arrest.setAwayTeamPlayerArrested(arrest.getDefensePlayerArrested());
        } else {
            // Defense is home team
            arrest.setHomeTeamPlayerArrested(arrest.getDefensePlayerArrested());
            arrest.setAwayTeamPlayerArrested(arrest.getOffensePlayerArrested());
        }
    }

    /**
     * Checks to see if the team has any arrests
     */
    private static void checkArrests(Play play, Arrest arrest, boolean checkOffense, HashMap<String, ArrayList<String>> teamSeasonToPlayersArrested) {
        boolean playerArrested = false;

        String season = play.getYear().toString();

        // Check for arrests
        ArrayList<String> arrestedPlayers = teamSeasonToPlayersArrested.get(getKey(season, checkOffense ? play.getOffense().toString() : play.getDefense().toString()));

        if (arrestedPlayers != null) {
            if (checkOffense) {
                arrest.setOffensePlayerArrested(true);

                playerArrested = wasPlayerArrested(play.getQB().toString(), play, arrestedPlayers);

                if (!playerArrested) {
                    playerArrested = wasPlayerArrested(play.getOffensivePlayer().toString(), play, arrestedPlayers);
                }

                arrest.setPlayerArrested(arrest.getPlayerArrested() || playerArrested);
            } else {
                playerArrested = wasPlayerArrested(play.getDefensivePlayer1().toString(), play, arrestedPlayers);

                if (!playerArrested && play.getDefensivePlayer2() != null) {
                    playerArrested = wasPlayerArrested(play.getDefensivePlayer2().toString(), play, arrestedPlayers);
                }

                arrest.setPlayerArrested(arrest.getPlayerArrested() || playerArrested);
            }
        }
    }

    private static boolean wasPlayerArrested(String player, Play play, ArrayList<String> arrestedPlayers) {
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
            if (play.getPlayDesc().toString().indexOf(firstInitial) != -1 || play.getPlayDesc().toString().indexOf(arrestedPlayer) != -1) {
                return true;
            }
        }

        return false;
    }

    public static String getKey(String season, String team) {
        return season + "-" + team;
    }
}
