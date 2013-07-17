nfldata
=======

The are two series of MapReduce programs.  One is a series of programs to extract and normalize the data.  The second is a simple program to look at incomplete passes.  

The play by play dataaset can be found at http://www.advancednflstats.com/2010/04/play-by-play-data.html.   

ETL Series
==========

This program takes the play by play dataset and merges it with other datasets like arrests, stadiums and weather.   

Run the PlayByPlayDriver on the play by data data.   
Run the ArrestJoinDriver on the data from PlayByPlayDriver.      
In Hive, run playbyplay_tablecreate.hql.    
In Hive, run playbyplay_join.hql.   
Query and have fun!   

Incomplete Passes
=================
Simple MapReduce on NFL play by play data.  This program focuses on incomplete passes and which receiver they were throw to.  See http://www.jesse-anderson.com/2013/01/nfl-play-by-play-analysis/ for the resulting charts.
