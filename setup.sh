#/bin/bash

BASEDIR=/user/cloudera

echo "Compiling MR"
cd src
javac -classpath `hadoop classpath` *.java
jar cf ../playbyplay.jar *.class
cd ..

echo "Deleting files in HDFS"
hadoop fs -rm -r $BASEDIR/input
hadoop fs -rm -r $BASEDIR/playoutput
hadoop fs -rm -r $BASEDIR/joinedoutput
hadoop fs -rm -r $BASEDIR/weather
hadoop fs -rm -r $BASEDIR/stadium

echo "Putting files in HDFS"
hadoop fs -put -f input $BASEDIR/input
hadoop fs -mkdir $BASEDIR/weather
hadoop fs -put -f 173328.csv $BASEDIR/weather/
hadoop fs -mkdir $BASEDIR/stadium
hadoop fs -put -f stadiums.csv $BASEDIR/stadium/
hadoop fs -put -f arrests.csv $BASEDIR/arrests.csv 

echo "Running MR Jobs"
hadoop jar playbyplay.jar PlayByPlayDriver $BASEDIR/input $BASEDIR/playoutput
hadoop jar playbyplay.jar ArrestJoinDriver $BASEDIR/playoutput $BASEDIR/joinedoutput $BASEDIR/arrests.csv

echo "Running Hive queries"
hive -S -f playbyplay_tablecreate.hql
hive -S -f playbyplay_join.hql
hive -S -f adddrives.hql
hive -S -f adddriveresult.hql

echo "All done importing the data"
echo ""
echo ""
echo "** Check the Hive and Pig queries in queries.hql and queries.pig **"
