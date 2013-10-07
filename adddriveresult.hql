ADD FILE drivesresulttransform.py;

drop table if exists playbyplay;

create table playbyplay
STORED AS TEXTFILE AS
SELECT playbyplay_drives.*, transformed.driveresult, transformed.maxplays FROM playbyplay_drives
join 
	-- Have to do a join this way because Hive does not support mixing transform and select *
	(SELECT TRANSFORM(game, drive, play, playtype, quarter, playid)
       USING 'drivesresulttransform.py'
       AS (driveresult STRING, maxplays INT, playid STRING)
       FROM 
       (select game, drive, play, playtype, quarter, playid from playbyplay_drives SORT BY playid DESC) orderedplays
     ) transformed
     ON (transformed.playid = playbyplay_drives.playid);
