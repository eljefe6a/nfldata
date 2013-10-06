drop table if exists playbyplay_weather;

create table playbyplay_weather
STORED AS TEXTFILE AS
select *,
(WV07 > 0 OR WV01 > 0 OR WV20 > 0 OR WV03 > 0) as hasWeatherInVicinity,
(WT09 > 0 OR WT14 > 0 OR WT07 > 0 OR WT01 > 0 OR WT15 > 0 OR WT17 > 0 OR 
WT06 > 0 OR WT21 > 0 OR WT05 > 0 OR  WT02 > 0 OR WT11 > 0 OR WT22 > 0 OR 
WT04 > 0 OR WT13 > 0 OR WT16 > 0 OR  WT08 > 0 OR WT18 > 0 OR WT03 > 0 OR 
WT10 > 0 OR WT19 > 0) as hasWeatherType,
(WV07 > 0 OR WV01 > 0 OR WV20 > 0 OR WV03 > 0 OR
WT09 > 0 OR WT14 > 0 OR WT07 > 0 OR WT01 > 0 OR WT15 > 0 OR WT17 > 0 OR 
WT06 > 0 OR WT21 > 0 OR WT05 > 0 OR  WT02 > 0 OR WT11 > 0 OR WT22 > 0 OR 
WT04 > 0 OR WT13 > 0 OR WT16 > 0 OR  WT08 > 0 OR WT18 > 0 OR WT03 > 0 OR 
WT10 > 0 OR WT19 > 0) as hasWeather
 from playbyplay_arrests
join stadium on stadium.team = playbyplay_arrests.hometeam
left outer join weather on stadium.weatherstation = weather.station and playbyplay_arrests.dateplayed = weather.readingdate;