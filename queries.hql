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
! echo "10 Miles per hour";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and awnd >= 44 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and awnd >= 44) totalstable 
order by playtype;

! echo "20 Miles per hour";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and awnd between 45 and 89 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and awnd between 45 and 89) totalstable 
order by playtype;

! echo "30 Miles per hour";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and awnd between 90 and 134 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and awnd between 90 and 134) totalstable 
order by playtype;

! echo "40 Miles per hour";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and awnd between 135 and 178 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and awnd between 135 and 178) totalstable 
order by playtype;

! echo "Above 40 Miles per hour";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and awnd > 178 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and awnd > 178) totalstable 
order by playtype;

! echo "****** WV and WT Play Types ******";
! echo "WV07 Ash, dust, sand, or other blowing obstruction";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WV07 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WV07 > 0) totalstable 
order by playtype;

! echo "WV01 Fog, ice fog, or freezing fog";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WV01 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WV01 > 0) totalstable 
order by playtype;

! echo "WV20 Rain or snow shower";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WV20 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WV20 > 0) totalstable 
order by playtype;

! echo "WV03 Thunder";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WV03 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WV03 > 0) totalstable 
order by playtype;

! echo "WT09 Blowing or drifting snow";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT09 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT09 > 0) totalstable 
order by playtype;

! echo "WT14 Drizzle";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT14 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT14 > 0) totalstable 
order by playtype;

! echo "WT07 Dust, volcanic ash, blowing dust, blowing sand, or blowing obstruction";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT07 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT07 > 0) totalstable 
order by playtype;

! echo "WT01 Fog, ice fog, or freezing fog";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT01 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT01 > 0) totalstable 
order by playtype;

! echo "WT15 Freezing drizzle";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT15 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT15 > 0) totalstable 
order by playtype;

! echo "WT17 Freezing rain";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT17 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT17 > 0) totalstable 
order by playtype;

! echo "WT06 Glaze or rime";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT06 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT06 > 0) totalstable 
order by playtype;

! echo "WT21 Ground fog";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT21 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT21 > 0) totalstable 
order by playtype;

! echo "WT05 Hail";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT05 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT05 > 0) totalstable 
order by playtype;

! echo "WT02 Heavy fog or heaving freezing fog";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT02 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT02 > 0) totalstable 
order by playtype;

! echo "WT11 High or damaging winds";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT11 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT11 > 0) totalstable 
order by playtype;

! echo "WT22 Ice fog or freezing fog";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT22 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT22 > 0) totalstable 
order by playtype;

! echo "WT04 Ice pellets, sleet, snow pellets, or small hail";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT04 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT04 > 0) totalstable 
order by playtype;

! echo "WT13 Mist";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT13 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT13 > 0) totalstable 
order by playtype;

! echo "WT16 Rain";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT16 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT16 > 0) totalstable 
order by playtype;

! echo "WT08 Smoke or haze";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT08 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT08 > 0) totalstable 
order by playtype;

! echo "WT18 Snow, snow pellets, snow grains, or ice crystals";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT18 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT18 > 0) totalstable 
order by playtype;

! echo "WT03 Thunder";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT03 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT03 > 0) totalstable 
order by playtype;

! echo "WT10 Tornado, waterspout, or funnel cloud";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT10 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT10 > 0) totalstable 
order by playtype;

! echo "WT19 Unknown source of precipitation";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and WT19 > 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and WT19 > 0) totalstable 
order by playtype;

! echo "****** Temperature ******";
! echo "Below 32 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and TMAX < 0 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and TMAX < 0) totalstable 
order by playtype;

! echo "45 to 33 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and TMAX between 1 and 70 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and TMAX between 1 and 70) totalstable 
order by playtype;

! echo "46 to 55 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and TMAX between 71 and 120 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and TMAX between 71 and 120) totalstable 
order by playtype;

! echo "56 to 65 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and TMAX between 121 and 180 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and TMAX between 135 and 178) totalstable 
order by playtype;

! echo "66 to 75 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and TMAX between 121 and 180 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and TMAX between 179 and 238) totalstable 
order by playtype;

! echo "76 to 85 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and TMAX between 239 and 294 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and TMAX between 239 and 1294) totalstable 
order by playtype;

! echo "86 to 95 F";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and TMAX between 295 and 350 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and TMAX between 295 and 350) totalstable 
order by playtype;

! echo "Above 96 Fr";
select playtype, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
(select playtype, count(*) as totalperplay from playbyplay where rooftype <> "None" and TMAX > 351 group by playtype) playbyplay 
 full outer join 
(select count(*) as total from playbyplay where rooftype <> "None" and TMAX > 351) totalstable 
order by playtype;

-- Play type by down

-- Play type by yards to go

-- Wins by teams with arrests comparison

-- Arrests by season

-- Wins by home team

-- Wins by home team when there is bad weather

-- Fumbles, penalty and incompletes by weather

-- QB Nemesis - Which defensive person had the most co-occurrence with a QB?

-- Crowd capacity and scoring

-- Scoring by weather type

-- Field goals makes by weather type

-- The break down of how many plays occur on which yard line

-- Scores by artificial turf and grass

