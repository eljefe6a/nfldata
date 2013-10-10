playbyplay = LOAD '/user/hive/warehouse/playbyplay' USING PigStorage('\u0001') as (
	game:chararray,
	quarter:int,
	gameminutes:int,
	gameseconds:int,
	offense:chararray,
	defense:chararray,
	down:int,
	yardstogo:int,
	yardline:int,
	playdesc:chararray,
	offensescore:int,
	defensescore:int,
	year:int,
	qb:chararray,
	offensiveplayer:chararray,
	defensiveplayer1:chararray,
	defensiveplayer2:chararray,
	penalty:boolean,
	fumble:boolean,
	incomplete:boolean,
	isgoalgood:boolean,
	playtype:chararray,
	hometeam:chararray,
	awayteam:chararray,
	dateplayed:chararray,
	playid:chararray,
	winner:chararray,
	hometeamscore:int,
	awayteamscore:int,
	playerarrested:boolean,
	offenseplayerarrested:boolean,
	defenseplayerarrested:boolean,
	hometeamplayerarrested:boolean,
	awayteamplayerarrested:boolean,
	stadium:chararray,
	capacity:int,
	expandedcapacity:int,
	stadiumlocation:chararray,
	playingsurface:chararray,
	isartificial:boolean,
	team:chararray,
	opened:int,
	weatherstation:chararray,
	rooftype:chararray,
	elevation:int,
	station:chararray,
	station_name:chararray,
	readingdate:chararray,
	mdpr:int,
	mdsf:int,
	dapr:int,
	prcp:int,
	snwd:int,
	snow:int,
	psun:int,
	tsun:int,
	tmax:int,
	tmin:int,
	tobs:int,
	wesd:int,
	wesf:int,
	awnd:int,
	wdf2:int,
	wdf5:int,
	wdfg:int,
	wsf2:int,
	wsf5:int,
	wsfg:int,
	pgtm:int,
	fmtm:int,
	wv07:int,
	wv01:int,
	wv20:int,
	wv03:int,
	wt09:int,
	wt14:int,
	wt07:int,
	wt01:int,
	wt15:int,
	wt17:int,
	wt06:int,
	wt21:int,
	wt05:int,
	wt02:int,
	wt11:int,
	wt22:int,
	wt04:int,
	wt13:int,
	wt16:int,
	wt08:int,
	wt18:int,
	wt03:int,
	wt10:int,
	wt19:int,
	hasweatherinvicinity:boolean,
	hasweathertype:boolean,
	hasweather:boolean,
	drive:int,
	play:int,
	driveresult:chararray,
	maxplays:int
);

playfiltered = FILTER playbyplay BY play == 1 and driveresult != 'KICKOFF' and prcp > 0;

playgroup = GROUP playfiltered ALL;

playaverages = FOREACH playgroup GENERATE AVG(playfiltered.maxplays), MAX(playfiltered.maxplays);

sh echo "Average number of plays in a drive and the maximum number of plays in a drive with precipitation";
DUMP playaverages;

-- Next query

firstplay = FILTER playbyplay BY play == 1 and driveresult != 'KICKOFF';

playbyyardline = GROUP firstplay BY yardline;

yardcounts = FOREACH playbyyardline GENERATE group, COUNT(firstplay) as totalperyard;

firstplaygroup = GROUP firstplay ALL;

firstplaycounts = FOREACH firstplaygroup GENERATE COUNT(firstplay) as totalplays;

yardscrossed = CROSS firstplaycounts, yardcounts;

yardscalculated = FOREACH yardscrossed GENERATE yardcounts::group, yardcounts::totalperyard, firstplaycounts::totalplays, 
	((float)yardcounts::totalperyard / (float)firstplaycounts::totalplays) * 100;

yardscalculatedsorted = ORDER yardscalculated by group;

sh echo "Yardline where drives start";
DUMP yardscalculatedsorted;
