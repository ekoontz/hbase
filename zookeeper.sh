export CLASSPATH=\
~/hbase/build/classes:\
~/stable/hbase-0.20.3/lib/commons-logging-1.0.4.jar:\
~/stable/hbase-0.20.3/lib/hadoop-0.20.1-hdfs127-core.jar:\
~/stable/hbase-0.20.3/lib/zookeeper-3.2.2.jar:\
~/stable/hbase-0.20.3/lib/log4j-1.2.15.jar

export HBASE_CLASSPATH=$CLASSPATH

export HBASE_HOME=~/hbase

export JAVA_HOME=/opt/jdk1.6.0_20/

export PATH=$H~/hbase/bin:$PATH

$HBASE_HOME/bin/hbase zookeeper start 


