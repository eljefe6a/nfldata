set hive.cli.print.header=true;
! echo "****** Precipitation ******";
! echo "Play break down without precipitation";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and prcp <= 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and prcp <= 0) totalstable 
order by playtype;

! echo "Play break down with precipitation";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and prcp > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and prcp > 0) totalstable 
order by playtype;

! echo "****** Wind Speeds ******";
! echo "0-9 Miles per hour";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and awnd < 44 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and awnd < 44) totalstable 
order by playtype;

! echo "10-19 Miles per hour";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and awnd >= 44 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and awnd >= 44) totalstable 
order by playtype;

! echo "20-29 Miles per hour";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and awnd between 45 and 89 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and awnd between 45 and 89) totalstable 
order by playtype;

! echo "30-39 Miles per hour";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and awnd between 90 and 134 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and awnd between 90 and 134) totalstable 
order by playtype;

! echo "Above 40 Miles per hour";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and awnd > 135 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and awnd > 135) totalstable 
order by playtype;

! echo "With roof";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None") totalstable 
order by playtype;

! echo "****** WV and WT Play Types ******";
! echo "WV07 Ash, dust, sand, or other blowing obstruction";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WV07 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WV07 > 0) totalstable 
order by playtype;

! echo "WV01 Fog, ice fog, or freezing fog";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WV01 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WV01 > 0) totalstable 
order by playtype;

! echo "WV20 Rain or snow shower";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WV20 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WV20 > 0) totalstable 
order by playtype;

! echo "WV03 Thunder";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WV03 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WV03 > 0) totalstable 
order by playtype;

! echo "WT09 Blowing or drifting snow";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT09 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT09 > 0) totalstable 
order by playtype;

! echo "WT14 Drizzle";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT14 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT14 > 0) totalstable 
order by playtype;

! echo "WT07 Dust, volcanic ash, blowing dust, blowing sand, or blowing obstruction";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT07 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT07 > 0) totalstable 
order by playtype;

! echo "WT01 Fog, ice fog, or freezing fog";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT01 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT01 > 0) totalstable 
order by playtype;

! echo "WT15 Freezing drizzle";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT15 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT15 > 0) totalstable 
order by playtype;

! echo "WT17 Freezing rain";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT17 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT17 > 0) totalstable 
order by playtype;

! echo "WT06 Glaze or rime";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT06 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT06 > 0) totalstable 
order by playtype;

! echo "WT21 Ground fog";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT21 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT21 > 0) totalstable 
order by playtype;

! echo "WT05 Hail";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT05 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT05 > 0) totalstable 
order by playtype;

! echo "WT02 Heavy fog or heaving freezing fog";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT02 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT02 > 0) totalstable 
order by playtype;

! echo "WT11 High or damaging winds";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT11 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT11 > 0) totalstable 
order by playtype;

! echo "WT22 Ice fog or freezing fog";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT22 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT22 > 0) totalstable 
order by playtype;

! echo "WT04 Ice pellets, sleet, snow pellets, or small hail";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT04 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT04 > 0) totalstable 
order by playtype;

! echo "WT13 Mist";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT13 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT13 > 0) totalstable 
order by playtype;

! echo "WT16 Rain";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT16 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT16 > 0) totalstable 
order by playtype;

! echo "WT08 Smoke or haze";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT08 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT08 > 0) totalstable 
order by playtype;

! echo "WT18 Snow, snow pellets, snow grains, or ice crystals";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT18 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT18 > 0) totalstable 
order by playtype;

! echo "WT03 Thunder";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT03 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT03 > 0) totalstable 
order by playtype;

! echo "WT10 Tornado, waterspout, or funnel cloud";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT10 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT10 > 0) totalstable 
order by playtype;

! echo "WT19 Unknown source of precipitation";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and WT19 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and WT19 > 0) totalstable 
order by playtype;

! echo "****** Temperature ******";
! echo "Below 32 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and TMAX < 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and TMAX < 0) totalstable 
order by playtype;

! echo "45 to 33 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and TMAX between 1 and 70 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and TMAX between 1 and 70) totalstable 
order by playtype;

! echo "46 to 55 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and TMAX between 71 and 120 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and TMAX between 71 and 120) totalstable 
order by playtype;

! echo "56 to 65 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and TMAX between 121 and 180 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and TMAX between 121 and 180) totalstable 
order by playtype;

! echo "66 to 75 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and TMAX between 181 and 238 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and TMAX between 181 and 238) totalstable 
order by playtype;

! echo "76 to 85 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and TMAX between 239 and 294 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and TMAX between 239 and 294) totalstable 
order by playtype;

! echo "86 to 95 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and TMAX between 295 and 350 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and TMAX between 295 and 350) totalstable 
order by playtype;

! echo "Above 96 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype = "None" and TMAX > 351 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype = "None" and TMAX > 351) totalstable 
order by playtype;

! echo "****** Play Type by Down ******";
! echo "First";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where down = 1 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where down = 1) totalstable 
order by playtype;

! echo "Second";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where down = 2 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where down = 2) totalstable 
order by playtype;

! echo "Third";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where down = 3 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where down = 3) totalstable 
order by playtype;

! echo "Fourth";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where down = 4 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where down = 4) totalstable 
order by playtype;

! echo "****** Play Type by Yards to Go ******";
! echo "All";
 select playbyplay.playtype, playbyplay.yardstogo, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
   (select playtype, yardstogo, count(*) as totalperplay from playbyplay group by yardstogo, playtype) playbyplay  
 join 
   (select yardstogo, count(*) as total from playbyplay group by yardstogo) totalstable
   on totalstable.yardstogo = playbyplay.yardstogo
 order by yardstogo, playtype;

! echo "Sack and Scrambles";
select playbyplay.playtype, playbyplay.yardstogo, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
  (select playtype, yardstogo, count(*) as totalperplay from playbyplay where (playtype = "SACK" OR playtype = "SCRAMBLE") group by yardstogo, playtype) playbyplay 
join 
  (select yardstogo, count(*) as total from playbyplay group by yardstogo) totalstable
  on totalstable.yardstogo = playbyplay.yardstogo
order by percentage;

! echo "****** Wins by Arrests ******";
! echo "All";
select hometeamwon, awayteamwon, HomeTeamPlayerArrested, AwayTeamPlayerArrested, playbyplay.totalperarrest, totalstable.total, ((playbyplay.totalperarrest / totalstable.total) * 100) as percentage from  
  (select hometeamwon, awayteamwon, HomeTeamPlayerArrested, AwayTeamPlayerArrested, count(*) as totalperarrest from
    (select distinct game, (winner = hometeam) as hometeamwon, (winner = awayteam) as awayteamwon, HomeTeamPlayerArrested, AwayTeamPlayerArrested from playbyplay) playbyplay
  group by hometeamwon, awayteamwon, HomeTeamPlayerArrested, AwayTeamPlayerArrested) playbyplay
full outer join 
(select count(*) as total from
  (select game from playbyplay group by game) playbyplay) totalstable;

! echo "****** Arrests by Season ******";
! echo "All";
select year, count(*) as totalperarrest from
  (select hometeam, year from playbyplay where HomeTeamPlayerArrested = true group by year, hometeam) playbyplay
group by year;

! echo "By team per season";
select hometeam, count(*) as totalperarrest from
  (select hometeam, year from playbyplay where HomeTeamPlayerArrested = true group by year, hometeam) playbyplay
group by hometeam
order by totalperarrest;

! echo "****** Wins by home team ******";
! echo "All";
select playbyplay.totalhometeamwins, totalstable.total, ((playbyplay.totalhometeamwins / totalstable.total) * 100) as percentage from  
  (select count(*) as totalhometeamwins from
    (select distinct game, (winner = hometeam) as hometeamwon from playbyplay) playbyplay
  where hometeamwon = true) playbyplay
full outer join 
  (select count(*) as total from
    (select distinct game from playbyplay) playbyplay
  ) totalstable;

! echo "****** Wins by home team when there is bad weather ******";
! echo "hasWeatherInVicinity";
select playbyplay.totalhometeamwins, totalstable.total, ((playbyplay.totalhometeamwins / totalstable.total) * 100) as percentage from  
  (select count(*) as totalhometeamwins from
    (select distinct game, (winner = hometeam) as hometeamwon from playbyplay where hasWeatherInVicinity = true) playbyplay
  where hometeamwon = true) playbyplay
full outer join 
  (select count(*) as total from
    (select distinct game from playbyplay where hasWeatherInVicinity = true) playbyplay
  ) totalstable;

! echo "hasWeatherType";
select playbyplay.totalhometeamwins, totalstable.total, ((playbyplay.totalhometeamwins / totalstable.total) * 100) as percentage from  
  (select count(*) as totalhometeamwins from
    (select distinct game, (winner = hometeam) as hometeamwon from playbyplay where hasWeatherType = true) playbyplay
  where hometeamwon = true) playbyplay
full outer join 
  (select count(*) as total from
    (select distinct game from playbyplay where hasWeatherType = true) playbyplay
  ) totalstable;

! echo "hasWeather";
select playbyplay.totalhometeamwins, totalstable.total, ((playbyplay.totalhometeamwins / totalstable.total) * 100) as percentage from  
  (select count(*) as totalhometeamwins from
    (select distinct game, (winner = hometeam) as hometeamwon from playbyplay where hasWeather = true) playbyplay
  where hometeamwon = true) playbyplay
full outer join 
  (select count(*) as total from
    (select distinct game from playbyplay where hasWeather = true) playbyplay
  ) totalstable;

! echo "****** Fumbles, penalty and incompletes by weather ******";
! echo "Plays with Weather";
select totalweatherfumbles, total, ((playbyplay.totalweatherfumbles / totalstable.total) * 100) as percentage from  
  (select count(*) as totalweatherfumbles from
    (select game from playbyplay where hasWeather = true) playbyplay)
   playbyplay
full outer join 
  (select count(*) as total from playbyplay) totalstable;

! echo "Fumbles by game";
select hasWeather, totalweatherfumbles, total, ((playbyplay.totalweatherfumbles / totalstable.total) * 100) as percentage from  
  (select hasWeather, count(*) as totalweatherfumbles from 
    (select game, hasWeather from playbyplay where fumble = true group by game, hasWeather) playbyplay
  group by hasWeather) playbyplay
full outer join 
  (select count(*) as total from
    (select game from playbyplay where fumble = true group by game) playbyplay
  ) totalstable;

! echo "Penalty by plays";
select playbyplay.hasWeather, totalweatherpenalty, total, ((playbyplay.totalweatherpenalty / totalstable.total) * 100) as percentage from  
  (select hasWeather, count(*) as totalweatherpenalty from
    (select hasWeather from playbyplay where penalty = true) playbyplay
   group by hasWeather)
   playbyplay
join 
  (select hasWeather, count(*) as total from playbyplay group by hasWeather) totalstable
on playbyplay.hasWeather = totalstable.hasWeather;

! echo "Incomplete by plays";
select playbyplay.hasWeather, totalweatherpenalty, total, ((playbyplay.totalweatherpenalty / totalstable.total) * 100) as percentage from  
  (select hasWeather, count(*) as totalweatherpenalty from
    (select hasWeather from playbyplay where incomplete = true) playbyplay
   group by hasWeather)
   playbyplay
join 
  (select hasWeather, count(*) as total from playbyplay group by hasWeather) totalstable
on playbyplay.hasWeather = totalstable.hasWeather;  

! echo "****** TODO: Nemesis - Offense and Defense co-occurrence ******";
! echo "QB";
--select OffensivePlayer, DefensivePlayer, total from  
--(
--  select OffensivePlayer, DefensivePlayer, count(*) as total from
--  (
--    (select OffensivePlayer, DefensivePlayer1 as DefensivePlayer from playbyplay where OffensivePlayer <> "" and DefensivePlayer1 <> "")
--  union all
--    (select OffensivePlayer, DefensivePlayer2 as DefensivePlayer from playbyplay where OffensivePlayer <> "" and DefensivePlayer2 <> "")
--   ) playbyplay
--   group by OffensivePlayer, DefensivePlayer
--) playbyplay
--order by total DESC limit 100;

! echo "OffensivePlayer";

! echo "****** Crowd capacity and scoring ******";
! echo "All";
select capacity, avg(HomeTeamScore) as homeTeamAverage, avg(AwayTeamScore) as awayTeamAverage from 
  (select game, capacity, homeTeamScore, AwayTeamScore from playbyplay group by game, capacity, homeTeamScore, AwayTeamScore) playbyplay
group by capacity
order by capacity desc;

! echo "****** Scoring by weather type ******";
! echo "hasWeatherInVicinity true";
select * from
  (select hometeam, hasWeatherInVicinity, avg(HomeTeamScore) as homeTeamAverage, avg(AwayTeamScore) as awayTeamAverage from 
    (select hometeam, homeTeamScore, AwayTeamScore, hasWeatherInVicinity from playbyplay
    where hasWeatherInVicinity = true and rooftype = "None"
    group by game, hometeam, homeTeamScore, AwayTeamScore, hasWeatherInVicinity) playbyplay
  group by hometeam, hasWeatherInVicinity) playbyplay
order by hometeam, hasWeatherInVicinity;

! echo "hasWeatherInVicinity false";
select * from
  (select hometeam, hasWeatherInVicinity, avg(HomeTeamScore) as homeTeamAverage, avg(AwayTeamScore) as awayTeamAverage from 
    (select hometeam, homeTeamScore, AwayTeamScore, hasWeatherInVicinity from playbyplay
    where hasWeatherInVicinity = false OR rooftype <> "None"
    group by game, hometeam, homeTeamScore, AwayTeamScore, hasWeatherInVicinity) playbyplay
  group by hometeam, hasWeatherInVicinity) playbyplay
order by hometeam, hasWeatherInVicinity;

! echo "hasWeatherType true";
select * from
  (select hometeam, hasWeatherType, avg(HomeTeamScore) as homeTeamAverage, avg(AwayTeamScore) as awayTeamAverage from 
    (select hometeam, homeTeamScore, AwayTeamScore, hasWeatherType from playbyplay
    where hasWeatherType = true and rooftype = "None"
    group by game, hometeam, homeTeamScore, AwayTeamScore, hasWeatherType) playbyplay
  group by hometeam, hasWeatherType) playbyplay
order by hometeam, hasWeatherType;

! echo "hasWeatherType false";
select * from
  (select hometeam, hasWeatherType, avg(HomeTeamScore) as homeTeamAverage, avg(AwayTeamScore) as awayTeamAverage from 
    (select hometeam, homeTeamScore, AwayTeamScore, hasWeatherType from playbyplay
    where hasWeatherType = false OR rooftype <> "None"
    group by game, hometeam, homeTeamScore, AwayTeamScore, hasWeatherType) playbyplay
  group by hometeam, hasWeatherType) playbyplay
order by hometeam, hasWeatherType;

! echo "hasWeather true";
select * from
  (select hometeam, hasWeather, avg(HomeTeamScore) as homeTeamAverage, avg(AwayTeamScore) as awayTeamAverage from 
    (select hometeam, homeTeamScore, AwayTeamScore, hasWeather from playbyplay
    where hasWeather = true and rooftype = "None"
    group by game, hometeam, homeTeamScore, AwayTeamScore, hasWeather) playbyplay
  group by hometeam, hasWeather) playbyplay
order by hometeam, hasWeather;

! echo "hasWeather false";
select * from
  (select hometeam, avg(HomeTeamScore) as homeTeamAverage, avg(AwayTeamScore) as awayTeamAverage from 
    (select hometeam, homeTeamScore, AwayTeamScore from playbyplay
    where hasWeather = false OR rooftype <> "None"
    group by game, hometeam, homeTeamScore, AwayTeamScore) playbyplay
  group by hometeam) playbyplay
order by hometeam;

! echo "****** Field goals makes by weather type ******";
! echo "hasWeatherInVicinity true";
select * from 
  (select IsGoalGood, hasWeatherInVicinity, count(*) from playbyplay
  where hasWeatherInVicinity = true AND rooftype = "None" AND (playtype = "FIELDGOAL") 
  group by IsGoalGood, hasWeatherInVicinity) playbyplay
order by IsGoalGood, hasWeatherInVicinity;

! echo "hasWeatherInVicinity false";
select * from 
  (select IsGoalGood, hasWeatherInVicinity, count(*) from playbyplay
  where (hasWeatherInVicinity = false OR rooftype <> "None") AND (playtype = "FIELDGOAL") 
  group by IsGoalGood, hasWeatherInVicinity) playbyplay
order by IsGoalGood, hasWeatherInVicinity;

! echo "hasWeatherType true";
select * from 
  (select IsGoalGood, hasWeatherType, count(*) from playbyplay
  where hasWeatherType = true AND rooftype = "None" AND (playtype = "FIELDGOAL") 
  group by IsGoalGood, hasWeatherType) playbyplay
order by IsGoalGood, hasWeatherType;

! echo "hasWeatherType false";
select * from 
  (select IsGoalGood, hasWeatherType, count(*) from playbyplay
  where (hasWeatherType = false OR rooftype <> "None") AND (playtype = "FIELDGOAL") 
  group by IsGoalGood, hasWeatherType) playbyplay
order by IsGoalGood, hasWeatherType;

! echo "hasWeather true";
select * from 
  (select IsGoalGood, hasWeather, count(*) from playbyplay
  where hasWeather = true AND rooftype = "None" AND (playtype = "FIELDGOAL") 
  group by IsGoalGood, hasWeather) playbyplay
order by IsGoalGood, hasWeather;

! echo "hasWeather false";
select * from 
  (select IsGoalGood, hasWeather, count(*) from playbyplay
  where (hasWeather = false OR rooftype <> "None") AND (playtype = "FIELDGOAL") 
  group by IsGoalGood, hasWeather) playbyplay
order by IsGoalGood, hasWeather;

! echo "****** The break down of how many plays occur on which yard line ******";
! echo "All";
select yardline, count(*) from playbyplay
group by yardline
order by yardline;

-- Scores by artificial turf and grass

-- Play types broken down by team

-- Stadium capacity and wins

-- Fumbles by down

! echo "****** Missed field goals by wind ******";
! echo "0-9 Miles per hour";
select IsGoalGood, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select IsGoalGood, count(*) as totalperplay from playbyplay where playtype = "FIELDGOAL" AND rooftype = "None" and awnd < 44 group by IsGoalGood) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where playtype = "FIELDGOAL" AND rooftype = "None" and awnd < 44) totalstable 
order by IsGoalGood;

! echo "10-19 Miles per hour";
select IsGoalGood, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select IsGoalGood, count(*) as totalperplay from playbyplay where playtype = "FIELDGOAL" AND rooftype = "None" and awnd >= 44 group by IsGoalGood) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where playtype = "FIELDGOAL" AND rooftype = "None" and awnd >= 44) totalstable 
order by IsGoalGood;

! echo "20-29 Miles per hour";
select IsGoalGood, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select IsGoalGood, count(*) as totalperplay from playbyplay where playtype = "FIELDGOAL" AND rooftype = "None" and awnd between 45 and 89 group by IsGoalGood) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where playtype = "FIELDGOAL" AND rooftype = "None" and awnd between 45 and 89) totalstable 
order by IsGoalGood;

! echo "30-39 Miles per hour";
select IsGoalGood, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select IsGoalGood, count(*) as totalperplay from playbyplay where playtype = "FIELDGOAL" AND rooftype = "None" and awnd between 90 and 134 group by IsGoalGood) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where playtype = "FIELDGOAL" AND rooftype = "None" and awnd between 90 and 134) totalstable 
order by IsGoalGood;

! echo "Above 40 Miles per hour";
select IsGoalGood, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select IsGoalGood, count(*) as totalperplay from playbyplay where playtype = "FIELDGOAL" AND rooftype = "None" and awnd > 135 group by IsGoalGood) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where playtype = "FIELDGOAL" AND rooftype = "None" and awnd > 135) totalstable 
order by IsGoalGood;

! echo "With roof";
select IsGoalGood, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select IsGoalGood, count(*) as totalperplay from playbyplay where playtype = "FIELDGOAL" AND rooftype <> "None" group by IsGoalGood) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where playtype = "FIELDGOAL" AND rooftype <> "None") totalstable 
order by IsGoalGood;

! echo "Play types by elevation";
! echo "Sea Level";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where elevation < 100 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where elevation < 100) totalstable 
order by playtype;

! echo "Mid Range";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where elevation between 100 and 5000 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where elevation between 100 and 5000) totalstable 
order by playtype;

! echo "Mountain";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where elevation > 5000 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where elevation > 5000) totalstable 
order by playtype;

! echo "Play results by yard started on";
! echo "By Play";
select playbyplay.yardline, playbyplay.driveresult, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select driveresult, yardline, count(*) as totalperplay from playbyplay where play = 1 and driveresult <> "KICKOFF" GROUP BY driveresult, yardline) playbyplay 
join 
  (select count(*) as total, yardline from playbyplay where play = 1 and driveresult <> "KICKOFF" GROUP BY yardline) totalstable 
  ON (totalstable.yardline = playbyplay.yardline)
order by yardline, driveresult;

! echo "By Scoring";
select playbyplay.yardline, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
  (select yardline, count(*) as totalperplay 
  from playbyplay where play = 1 and (driveresult = "EXTRAPOINT" OR driveresult = "FIELDGOAL") AND driveresult <> "KICKOFF"
  GROUP BY yardline) playbyplay 
join 
  (select count(*) as total, yardline from playbyplay where play = 1 AND driveresult <> "KICKOFF" GROUP BY yardline) totalstable 
  ON (totalstable.yardline = playbyplay.yardline)
order by yardline;

! echo "Starting at 80 yardline and up";
select playbyplay.driveresult, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select driveresult, count(*) as totalperplay from playbyplay where play = 1 and driveresult <> "KICKOFF" and yardline >= 80 GROUP BY driveresult) playbyplay 
full outer join
  (select count(*) as total from playbyplay where play = 1 and yardline >= 80 and driveresult <> "KICKOFF") totalstable 
order by driveresult;

! echo "Starting at 20 to 80 yardline";
select playbyplay.driveresult, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select driveresult, count(*) as totalperplay from playbyplay where play = 1 and driveresult <> "KICKOFF" and yardline < 80 and yardline > 20 GROUP BY driveresult) playbyplay 
full outer join
  (select count(*) as total from playbyplay where play = 1 and yardline < 80 and yardline > 20 and driveresult <> "KICKOFF") totalstable 
order by driveresult;

! echo "Red Zone starting at 0 to 20 yardline";
select playbyplay.driveresult, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select driveresult, count(*) as totalperplay from playbyplay where play = 1 and driveresult <> "KICKOFF" and yardline <= 20 GROUP BY driveresult) playbyplay 
full outer join
  (select count(*) as total from playbyplay where play = 1 and yardline <= 20 and driveresult <> "KICKOFF") totalstable 
order by driveresult;

! echo "All Yardlines";
select playbyplay.driveresult, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select driveresult, count(*) as totalperplay from playbyplay where play = 1 and driveresult <> "KICKOFF" GROUP BY driveresult) playbyplay 
full outer join
  (select count(*) as total from playbyplay where play = 1 and driveresult <> "KICKOFF") totalstable 
order by driveresult;

! echo "Drives";
! echo "Number of drives";
select count(*) from playbyplay where play = 1 and driveresult <> "KICKOFF";

! echo "Breakdown of results for all drives";
select playbyplay.driveresult, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select driveresult, count(*) as totalperplay from playbyplay where play = 1 and driveresult <> "KICKOFF" GROUP BY driveresult) playbyplay 
full outer join
  (select count(*) as total from playbyplay where play = 1 and driveresult <> "KICKOFF") totalstable 
order by driveresult;

! echo "Yardline where drives start";
select playbyplay.yardline, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select yardline, count(*) as totalperplay from playbyplay where play = 1 and driveresult <> "KICKOFF" GROUP BY yardline) playbyplay 
full outer join
  (select count(*) as total from playbyplay where play = 1 and driveresult <> "KICKOFF") totalstable 
order by yardline;

! echo "Drives and Plays";
! echo "Average number of plays in a drive and the maximum number of plays in a drive";
select avg(maxplays), max(maxplays) from playbyplay where play = 1 and driveresult <> "KICKOFF";

! echo "Average number of plays in a drive and the maximum number of plays in a drive broken down by stadium";
select stadium, avg(maxplays), max(maxplays) from playbyplay where play = 1 and driveresult <> "KICKOFF" GROUP BY stadium order by stadium;

! echo "Average number of plays in a drive and the maximum number of plays in a drive broken down by team";
select offense, avg(maxplays), max(maxplays) from playbyplay where play = 1 and driveresult <> "KICKOFF" GROUP BY offense order by offense;

! echo "Average number of plays in a drive and the maximum number of plays in a drive with weather";
select avg(maxplays), max(maxplays) from playbyplay where play = 1 and driveresult <> "KICKOFF" GROUP BY hasweather;

! echo "Average number of plays in a drive and the maximum number of plays in a drive broken down by stadium with weather";
select stadium, hasweather, avg(maxplays), max(maxplays) from playbyplay where play = 1 and driveresult <> "KICKOFF" GROUP BY hasweather, stadium order by stadium, hasweather;

! echo "Average number of plays in a drive and the maximum number of plays in a drive broken down by team with weather";
select offense, hasweather, avg(maxplays), max(maxplays) from playbyplay where play = 1 and driveresult <> "KICKOFF" GROUP BY hasweather, offense order by offense, hasweather;

! echo "Average number of plays in a drive and the maximum number of plays in a drive with precipitation";
select avg(maxplays), max(maxplays) from playbyplay where play = 1 and driveresult <> "KICKOFF" and prcp > 0;

! echo "Touchbacks";
! echo "Touchbacks by year";
select playbyplay.year, playbyplay.totaltouchbackperyear, totalstable.totalperyear, ((playbyplay.totaltouchbackperyear / totalstable.totalperyear) * 100) as percentage from  
  (select count(*) as totaltouchbackperyear, year from playbyplay where playtype = "KICKOFF" and LOWER(playdesc) LIKE "%touchback%" GROUP BY year) playbyplay 
join
  (select count(*) as totalperyear, year from playbyplay where playtype = "KICKOFF" GROUP BY year) totalstable 
on (totalstable.year = playbyplay.year)
order by year;
