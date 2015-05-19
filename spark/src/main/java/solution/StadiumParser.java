package solution;

import model.Stadium;
import org.apache.commons.lang.math.NumberUtils;

/**
 * Created by vmuser on 5/19/15.
 */
public class StadiumParser {
    public static Stadium parseStadium(String line) {
        String[] fields = line.split(",");

        Stadium stadium = new Stadium();
        stadium.setStadium(fields[0]);
        stadium.setCapacity(NumberUtils.toInt(fields[1], 0));
        stadium.setExpandedCapacity(NumberUtils.toInt(fields[2], 0));
        stadium.setStadiumLocation(fields[3]);
        stadium.setPlayingSurface(fields[4]);
        stadium.setIsArtificial(Boolean.valueOf(fields[5]));
        stadium.setTeam(fields[6]);
        stadium.setOpened(NumberUtils.toInt(fields[7], 0));
        stadium.setWeatherStation(fields[8]);

        // TODO: Change to enum
        //RoofType STRING COMMENT '(Possible Values:None,Retractable,Dome) - The type of roof in the stadium',
        stadium.setRoofType(fields[9]);
        stadium.setElevation(NumberUtils.toInt(fields[10], 0));

        return stadium;
    }
}
