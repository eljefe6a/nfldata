drop table if exists playbyplay;

create table playbyplay
STORED AS TEXTFILE AS
select *,
(WV07 == "-9990" OR WV01 == "-9999" OR WV20 == "-9999" OR WV03 == "-9999") as hasWeatherInVicinity,
(WT09 == "-9990" OR WT14 == "-9990" OR WT07 == "-9990" OR WT01 == "-9990" OR WT15 == "-9990" OR WT17 == "-9990" OR 
WT06 == "-9990" OR WT21 == "-9990" OR WT05 == "-9990" OR  WT02 == "-9990" OR WT11 == "-9990" OR WT22 == "-9990" OR 
WT04 == "-9990" OR WT13 == "-9990" OR WT16 == "-9990" OR  WT08 == "-9990" OR WT18 == "-9990" OR WT03 == "-9990" OR 
WT10 == "-9990" OR WT19 == "-9990") as hasWeatherType,
(WV07 == "-9990" OR WV01 == "-9999" OR WV20 == "-9999" OR WV03 == "-9999" OR
WT09 == "-9990" OR WT14 == "-9990" OR WT07 == "-9990" OR WT01 == "-9990" OR WT15 == "-9990" OR WT17 == "-9990" OR 
WT06 == "-9990" OR WT21 == "-9990" OR WT05 == "-9990" OR  WT02 == "-9990" OR WT11 == "-9990" OR WT22 == "-9990" OR 
WT04 == "-9990" OR WT13 == "-9990" OR WT16 == "-9990" OR  WT08 == "-9990" OR WT18 == "-9990" OR WT03 == "-9990" OR 
WT10 == "-9990" OR WT19 == "-9990") as hasWeather
 from playbyplay_arrests
join stadium on stadium.team = playbyplay_arrests.hometeam
left outer join weather on stadium.weatherstation = weather.station and playbyplay_arrests.dateplayed = weather.readingdate;