export CLASSPATH=\
/home/ekoontz/hbase/core/target/classes:\
/home/ekoontz/stable/hbase-0.20.3/lib/commons-logging-1.0.4.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/hadoop-0.20.1-hdfs127-core.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/zookeeper-3.2.2.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/log4j-1.2.15.jar

export HBASE_CLASSPATH=$CLASSPATH

export HBASE_HOME=/home/ekoontz/hbase

export JAVA_HOME=/opt/jdk1.6.0_20/

export PATH=/home/ekoontz/hbase/bin:$PATH

hbase zookeeper start 


