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
	Year INT COMMENT 'The year of the season',
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
	PlayId STRING COMMENT 'The unique id of the play',
	Winner STRING COMMENT 'The name of the team that eventually wins',
	HomeTeamScore INT COMMENT 'The home teams score at the end of the game',
	AwayTeamScore INT COMMENT 'The away teams score at the end of the game',
	PlayerArrested BOOLEAN COMMENT 'Was a player in the play arrested that season',
	OffensePlayerArrested BOOLEAN COMMENT 'Offense had player arrested in season',
	DefensePlayerArrested BOOLEAN COMMENT 'Defense had player arrested in season',	
	HomeTeamPlayerArrested BOOLEAN COMMENT 'Home team had player arrested in season',
	AwayTeamPlayerArrested BOOLEAN COMMENT 'Away team had player arrested in season'
	)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION "/user/cloudera/joinedoutput";

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
	RoofType STRING COMMENT '(Possible Values:None,Retractable,Dome) - The type of roof in the stadium',
	Elevation INT COMMENT 'The altitude of the stadium'
	)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/cloudera/stadium";


drop table if exists weather;
CREATE EXTERNAL TABLE weather (
	STATION STRING COMMENT 'Station identifier',
	STATION_NAME STRING COMMENT 'Station location name',
	READINGDATE STRING COMMENT 'Date of reading',
	MDPR INT COMMENT 'Multiday precipitation total (tenths of mm use with DAPR and DWPR, if available)',
	MDSF INT COMMENT 'Multiday snowfall total ',
	DAPR INT COMMENT 'Number of days included in the multiday precipitation total (MDPR)',
	PRCP INT COMMENT 'Precipitation (tenths of mm)',
	SNWD INT COMMENT 'Snow depth (mm)',
	SNOW INT COMMENT 'Snowfall (mm)',
	PSUN INT COMMENT 'Daily percent of possible sunshine (percent)',
	TSUN INT COMMENT 'Daily total sunshine (minutes)',
	TMAX INT COMMENT 'Maximum temperature (tenths of degrees C)',
	TMIN INT COMMENT 'Minimum temperature (tenths of degrees C)',
	TOBS INT COMMENT 'Temperature at the time of observation (tenths of degrees C)',
	WESD INT COMMENT 'Water equivalent of snow on the ground (tenths of mm)',
	WESF INT COMMENT 'Water equivalent of snowfall (tenths of mm)',
	AWND INT COMMENT 'Average daily wind speed (tenths of meters per second)',
	WDF2 INT COMMENT 'Direction of fastest 2-minute wind (degrees)',
	WDF5 INT COMMENT 'Direction of fastest 5-second wind (degrees)',
	WDFG INT COMMENT 'Direction of peak wind gust (degrees)',
	WSF2 INT COMMENT 'Fastest 2-minute wind speed (tenths of meters per second)',
	WSF5 INT COMMENT 'Fastest 5-second wind speed (tenths of meters per second)',
	WSFG INT COMMENT 'Peak guest wind speed (tenths of meters per second)',
	PGTM INT COMMENT 'Peak gust time (hours and minutes, i.e., HHMM)',
	FMTM INT COMMENT 'Time of fastest mile or fastest 1-minute wind (hours and minutes, i.e, HHMM)',
	WV07 INT COMMENT 'Ash, dust, sand, or other blowing obstruction',
	WV01 INT COMMENT 'Fog, ice fog, or freezing fog (may include heavy fog)',
	WV20 INT COMMENT 'Rain or snow shower',
	WV03 INT COMMENT 'Thunder',
	WT09 INT COMMENT 'Blowing or drifting snow',
	WT14 INT COMMENT 'Drizzle',
	WT07 INT COMMENT 'Dust, volcanic ash, blowing dust, blowing sand, or blowing obstruction',
	WT01 INT COMMENT 'Fog, ice fog, or freezing fog (may include heavy fog)',
	WT15 INT COMMENT 'Freezing drizzle ',
	WT17 INT COMMENT 'Freezing rain ',
	WT06 INT COMMENT 'Glaze or rime ',
	WT21 INT COMMENT 'Ground fog ',
	WT05 INT COMMENT 'Hail (may include small hail)',
	WT02 INT COMMENT 'Heavy fog or heaving freezing fog (not always distinguished from fog)',
	WT11 INT COMMENT 'High or damaging winds',
	WT22 INT COMMENT 'Ice fog or freezing fog',
	WT04 INT COMMENT 'Ice pellets, sleet, snow pellets, or small hail',
	WT13 INT COMMENT 'Mist',
	WT16 INT COMMENT 'Rain (may include freezing rain, drizzle, and freezing drizzle)',
	WT08 INT COMMENT 'Smoke or haze ',
	WT18 INT COMMENT 'Snow, snow pellets, snow grains, or ice crystals',
	WT03 INT COMMENT 'Thunder',
	WT10 INT COMMENT 'Tornado, waterspout, or funnel cloud',
	WT19 INT COMMENT 'Unknown source of precipitation '
	)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/cloudera/weather";