ADD FILE drivestransform.py;

drop table if exists playbyplay_drives;

create table playbyplay_drives
STORED AS TEXTFILE AS
SELECT playbyplay_weather.*, transformed.drive, transformed.play FROM playbyplay_weather
join 
	-- Have to do a join this way because Hive does not support mixing transform and select *
	(SELECT TRANSFORM(game, quarter, offense, gameminutes, gameseconds, playdesc)
       USING 'drivestransform.py'
       AS (drive INT, play INT, game STRING, quarter INT, gameminutes INT, gameseconds INT, playdesc STRING)
       FROM 
       (select game, quarter, offense, gameminutes, gameseconds, playdesc from playbyplay_weather
       SORT BY game, quarter, gameminutes DESC, gameseconds DESC) sortedtransform
     ) transformed
     ON (transformed.game = playbyplay_weather.game AND
     transformed.quarter = playbyplay_weather.quarter AND
     transformed.gameminutes = playbyplay_weather.gameminutes AND
     transformed.gameseconds = playbyplay_weather.gameseconds AND
     transformed.playdesc = playbyplay_weather.playdesc);
