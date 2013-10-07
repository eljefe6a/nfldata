ADD FILE drivesresulttransform.py;

drop table if exists playbyplay;

create table playbyplay
STORED AS TEXTFILE AS
SELECT playbyplay_drives.*, transformed.driveresult, transformed.maxplays FROM playbyplay_drives
join 
	-- Have to do a join this way because Hive does not support mixing transform and select *
	(SELECT TRANSFORM(game, drive, play, playtype, quarter, gameminutes, gameseconds, playdesc)
       USING 'drivesresulttransform.py'
       AS (driveresult STRING, maxplays INT, game STRING, drive INT, play INT, quarter INT, gameminutes INT, gameseconds INT, playdesc STRING)
       FROM playbyplay_drives SORT BY game, drive DESC, play DESC
     ) transformed
     ON (transformed.game = playbyplay_drives.game AND
     transformed.quarter = playbyplay_drives.quarter AND
     transformed.gameminutes = playbyplay_drives.gameminutes AND
     transformed.gameseconds = playbyplay_drives.gameseconds AND
     transformed.playdesc = playbyplay_drives.playdesc);
