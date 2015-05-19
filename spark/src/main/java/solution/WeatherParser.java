package solution;

import model.Weather;
import org.apache.commons.lang.math.NumberUtils;

/**
 * Created by vmuser on 5/19/15.
 */
public class WeatherParser {

    public static Weather parseWeather(String line) {
        String[] fields = line.split(",");

        Weather weather = new Weather();

        weather.setSTATION(fields[0]);
        weather.setSTATIONNAME(fields[1]);
        weather.setREADINGDATE(fields[2]);

        weather.setMDPR(NumberUtils.toInt(fields[3], 0));
        weather.setMDSF(NumberUtils.toInt(fields[4], 0));
        weather.setDAPR(NumberUtils.toInt(fields[5], 0));
        weather.setPRCP(NumberUtils.toInt(fields[6], 0));
        weather.setSNWD(NumberUtils.toInt(fields[7], 0));
        weather.setSNOW(NumberUtils.toInt(fields[8], 0));
        weather.setPSUN(NumberUtils.toInt(fields[9], 0));
        weather.setTSUN(NumberUtils.toInt(fields[10], 0));
        weather.setTMAX(NumberUtils.toInt(fields[11], 0));
        weather.setTMIN(NumberUtils.toInt(fields[12], 0));
        weather.setTOBS(NumberUtils.toInt(fields[13], 0));
        weather.setWESD(NumberUtils.toInt(fields[14], 0));
        weather.setWESF(NumberUtils.toInt(fields[15], 0));
        weather.setAWND(NumberUtils.toInt(fields[16], 0));
        weather.setWDF2(NumberUtils.toInt(fields[17], 0));
        weather.setWDF5(NumberUtils.toInt(fields[18], 0));
        weather.setWDFG(NumberUtils.toInt(fields[19], 0));
        weather.setWSF2(NumberUtils.toInt(fields[20], 0));
        weather.setWSF5(NumberUtils.toInt(fields[21], 0));
        weather.setPGTM(NumberUtils.toInt(fields[22], 0));
        weather.setFMTM(NumberUtils.toInt(fields[23], 0));
        weather.setWV07(NumberUtils.toInt(fields[24], 0));
        weather.setWV01(NumberUtils.toInt(fields[25], 0));
        weather.setWV20(NumberUtils.toInt(fields[26], 0));
        weather.setWV03(NumberUtils.toInt(fields[27], 0));
        weather.setWT09(NumberUtils.toInt(fields[28], 0));
        weather.setWT14(NumberUtils.toInt(fields[29], 0));
        weather.setWT07(NumberUtils.toInt(fields[30], 0));
        weather.setWT01(NumberUtils.toInt(fields[31], 0));
        weather.setWT15(NumberUtils.toInt(fields[32], 0));
        weather.setWT17(NumberUtils.toInt(fields[33], 0));
        weather.setWT06(NumberUtils.toInt(fields[34], 0));
        weather.setWT21(NumberUtils.toInt(fields[35], 0));
        weather.setWT05(NumberUtils.toInt(fields[36], 0));
        weather.setWT02(NumberUtils.toInt(fields[37], 0));
        weather.setWT11(NumberUtils.toInt(fields[38], 0));
        weather.setWT22(NumberUtils.toInt(fields[39], 0));
        weather.setWT04(NumberUtils.toInt(fields[40], 0));
        weather.setWT13(NumberUtils.toInt(fields[41], 0));
        weather.setWT16(NumberUtils.toInt(fields[42], 0));
        weather.setWT08(NumberUtils.toInt(fields[43], 0));
        weather.setWT18(NumberUtils.toInt(fields[44], 0));
        weather.setWT03(NumberUtils.toInt(fields[45], 0));
        weather.setWT10(NumberUtils.toInt(fields[46], 0));
        weather.setWT19(NumberUtils.toInt(fields[47], 0));

        return weather;
    }
}
