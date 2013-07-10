drop table if exists playbyplay_arrests;
CREATE EXTERNAL TABLE playbyplay_arrests (
	Game STRING COMMENT 'Game Id',
	Quarter INT COMMENT 'Game Quarter',
	GameMinutes INT COMMENT 'Game time countdown left in minutes',
	GameSeconds INT COMMENT 'Game time countdown left in seconds',
	Offense STRING COMMENT 'Team on offense',
	Defense STRING COMMENT 'Team on defense',
	Down INT COMMENT 'Down number',
	YardsToGo INT COMMENT 'Number of yards for a first down',
	YardLine INT COMMENT 'Yard line where the ball is',
	PlayDesc STRING COMMENT 'The original description of the play',
	OffenseScore INT COMMENT 'The offenses score as of the current play',
	DefenseScore INT COMMENT 'The defenses score as of the current play',
	QB STRING COMMENT 'The QB/Punter/Kicker in a play',
	OffensivePlayer STRING COMMENT 'The receiver or runner',
	DefensivePlayer1 STRING COMMENT 'The name of the defensive player on the play',
	DefensivePlayer2 STRING COMMENT 'The name of the other defensive player on the play',
	Penalty BOOLEAN COMMENT 'Whether or not there was a penalty on the play',
	Fumble BOOLEAN COMMENT 'Whether or not there was a fumble on the play',
	Incomplete BOOLEAN COMMENT 'Whether or not there was an incomplete pass on the play',
	IsGoalGood BOOLEAN COMMENT 'For a extra point or field goal kick, whether or not there it was good',
	PlayType STRING COMMENT '(Possible Values:PASS,INTERCEPTION,PASS,PUNT,RUN,KICKOFF,SPIKE,FIELDGOAL,EXTRAPOINT,PENALTY,FUMBLE,SACK,KNEEL,REVIEWSCRAMBLE,END) - The type of play that was run',
	HomeTeam STRING COMMENT 'The name of the home team',
	AwayTeam STRING COMMENT 'The name of the away team',
	DatePlayed STRING COMMENT 'The data of the game',
	Winner STRING COMMENT 'The name of the team that eventually wins',
	Season INT COMMENT 'Arrested in (February to February)',
	Team STRING COMMENT 'Team person played on',
	Player STRING COMMENT 'Name of player Arrested',
	PlayerArrested BOOLEAN COMMENT 'Was a player in the play arrested that season',
	DefensePlayerArrested BOOLEAN COMMENT 'Offense had player arrested in season',
	OffensePlayerArrested BOOLEAN COMMENT 'Defense had player arrested in season'
	)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION "/user/training/joinedoutput";

drop table if exists stadium;
CREATE EXTERNAL TABLE stadium (
	Stadium STRING COMMENT 'The name of the stadium',
	Capacity INT COMMENT 'The capacity of the stadium',
	ExpandedCapacity INT COMMENT 'The expanded capacity of the stadium',
	StadiumLocation STRING COMMENT 'The location of the stadium',
	PlayingSurface STRING COMMENT 'The type of grass, etc that the stadium has',
	IsArtificial BOOLEAN COMMENT 'Is the playing surface artificial',
	Team STRING COMMENT 'The name of the team that plays at the stadium',
	Opened INT COMMENT 'The year the stadium opened',
	WeatherStation STRING COMMENT 'The name of the weather station closest to the stadium',
	RoofType STRING COMMENT '(Possible Values:None,Retractable,Dome) - The type of roof in the stadium'
	)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/training/stadium";


drop table if exists weather;
CREATE EXTERNAL TABLE weather (
	READINGDATE STRING COMMENT 'Date of reading',
	PRCP STRING COMMENT 'Precipitation (tenths of mm)',
	SNOW STRING COMMENT 'Snowfall (mm)',
	TMAX STRING COMMENT 'Maximum temperature (tenths of degrees C)',
	TMIN STRING COMMENT 'Minimum temperature (tenths of degrees C)',
	TOBS STRING COMMENT 'Temperature at the time of observation (tenths of degrees C)',
	AWND STRING COMMENT 'Average daily wind speed (tenths of meters per second)',
	WT01 STRING COMMENT 'Fog, ice fog, or freezing fog (may include heavy fog)',
	WT02 STRING COMMENT 'Heavy fog or heaving freezing fog (not always distinguished from fog)',
	WT03 STRING COMMENT 'Thunder',
	WT04 STRING COMMENT 'Ice pellets, sleet, snow pellets, or small hail',
	WT05 STRING COMMENT 'Hail (may include small hail)',
	WT06 STRING COMMENT 'Glaze or rime',
	WT07 STRING COMMENT 'Dust, volcanic ash, blowing dust, blowing sand, or blowing obstruction',
	WT08 STRING COMMENT 'Smoke or haze ',
	WT09 STRING COMMENT 'Blowing or drifting snow',
	WT10 STRING COMMENT 'Tornado, waterspout, or funnel cloud',
	WT11 STRING COMMENT 'High or damaging winds',
	WT13 STRING COMMENT 'Mist',
	WT14 STRING COMMENT 'Drizzle',
	WT15 STRING COMMENT 'Freezing drizzle', 
	WT16 STRING COMMENT 'Rain (may include freezing rain, drizzle, and freezing drizzle)', 
	WT17 STRING COMMENT 'Freezing rain',
	WT18 STRING COMMENT 'Snow, snow pellets, snow grains, or ice crystals'
	)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/training/weather";