export CLASSPATH=\
~/hbase/build/classes:\
~/hbase/lib/commons-logging-1.0.4.jar:\
~/hbase/lib/hadoop-0.20.1-hdfs127-core.jar:\
~/hbase/lib/zookeeper-3.2.2.jar:\
~/hbase/lib/log4j-1.2.15.jar

export HBASE_CLASSPATH=$CLASSPATH

export HBASE_HOME=$HOME/hbase

export JAVA_HOME=/opt/jdk1.6.0_20/

export PATH=$HBASE_HOME/bin:$PATH

$HBASE_HOME/bin/hbase zookeeper start 


