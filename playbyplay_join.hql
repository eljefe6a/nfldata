drop table if exists playbyplay;

create table playbyplay
STORED AS TEXTFILE AS
select * from playbyplay_arrests
join stadium on stadium.team = playbyplay_arrests.hometeam
join weather on stadium.weatherstation = weather.station and playbyplay_arrests.dateplayed = weather.readingdate; 
