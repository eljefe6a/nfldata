#/bin/bash

echo "Compiling MR"
cd src
javac -classpath `hadoop classpath` *.java
jar cf ../playbyplay.jar *.class
cd ..

echo "Deleting files in HDFS"
hadoop fs -rm -r input
hadoop fs -rm -r playoutput
hadoop fs -rm -r joinedoutput
hadoop fs -rm -r weather
hadoop fs -rm -r stadium

echo "Putting files in HDFS"
hadoop fs -put -f input
hadoop fs -mkdir weather
hadoop fs -put -f 173328.csv weather/
hadoop fs -mkdir stadium
hadoop fs -put -f stadiums.csv stadium/
hadoop fs -put -f arrests.csv 

echo "Running MR Jobs"
hadoop jar playbyplay.jar PlayByPlayDriver input playoutput
hadoop jar playbyplay.jar ArrestJoinDriver playoutput joinedoutput arrests.csv

echo "Running Hive queries"
hive -S -f playbyplay_tablecreate.hql
hive -S -f playbyplay_join.hql
hive -S -f adddrives.hql
hive -S -f adddriveresult.hql

echo "All done importing the data"
echo ""
echo ""
echo "** Check the Hive and Pig queries in queries.hql and queries.pig **"
