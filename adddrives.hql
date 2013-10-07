ADD FILE drivestransform.py;

drop table if exists playbyplay_drives;

create table playbyplay_drives
STORED AS TEXTFILE AS
SELECT playbyplay_weather.*, transformed.drive, transformed.play FROM playbyplay_weather
join 
	-- Have to do a join this way because Hive does not support mixing transform and select *
	(SELECT TRANSFORM(game, quarter, offense, gameminutes, gameseconds, playid)
       USING 'drivestransform.py'
       AS (drive INT, play INT, playid STRING)
       FROM 
       (select game, quarter, offense, gameminutes, gameseconds, playid from playbyplay_weather
       SORT BY playid, game) sortedtransform
     ) transformed
     ON (transformed.playid = playbyplay_weather.playid);
