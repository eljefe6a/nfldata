#/bin/bash

BASEDIR=/user/cloudera
BASEREPO=repo:hive:$BASEDIR

LIBJARS=~/.m2/repository/org/apache/avro/avro-mapred/1.7.5-cdh5.0.0/avro-mapred-1.7.5-cdh5.0.0.jar

echo "Compiling MR"
mvn package

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
# mvn kite:run-tool -Dkite.toolClass=PlayByPlayDriver -Dkite.args="$BASEDIR/input,$BASEREPO/playoutput
hadoop jar target/playbyplay-1.0-SNAPSHOT.jar PlayByPlayDriver -libjars $LIBJARS $BASEDIR/input $BASEREPO/playoutput
hadoop jar target/playbyplay-1.0-SNAPSHOT.jar -libjars $LIBJARS ArrestJoinDriver $BASEREPO/playoutput $BASEREPO/joinedoutput $BASEDIR/arrests.csv

echo "Running Hive queries"
hive -f playbyplay_tablecreate.hql
echo "Doing play, stadium and weather joins"
hive -f playbyplay_join.hql
echo "Adding drives"
hive -f adddrives.hql
echo "Add drive results"
hive -f adddriveresult.hql

echo "All done importing the data"
echo ""
echo ""
echo "** Check the Hive and Pig queries in queries.hql and queries.pig **"
