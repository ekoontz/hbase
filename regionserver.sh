export CLASSPATH=\
~/hbase/build/classes:\
~/hbase/core/target/classes:\
~/hbase/conf:/opt/jdk1.6.0_20/lib/tools.jar:\
~/hbase:~/hbase/hbase-0.20.3.jar:\
~/hbase/conf:/opt/jdk1.6.0_20/lib/tools.jar:\
~/hbase:\
~/hbase/hbase-0.20.3-test.jar:\
~/hbase/lib/AgileJSON-2009-03-30.jar:\
~/hbase/lib/commons-cli-2.0-SNAPSHOT.jar:\
~/hbase/lib/commons-el-from-jetty-5.1.4.jar:\
~/hbase/lib/commons-httpclient-3.0.1.jar:\
~/hbase/lib/commons-logging-1.0.4.jar:\
~/hbase/lib/commons-logging-api-1.0.4.jar:\
~/hbase/lib/commons-math-1.1.jar:\
~/hbase/lib/hadoop-0.20.1-hdfs127-core.jar:\
~/hbase/lib/hadoop-0.20.1-test.jar:\
~/hbase/lib/jasper-compiler-5.5.12.jar:\
~/hbase/lib/jasper-runtime-5.5.12.jar:\
~/hbase/lib/jetty-6.1.14.jar:\
~/hbase/lib/jetty-util-6.1.14.jar:\
~/hbase/lib/jruby-complete-1.2.0.jar:\
~/hbase/lib/json.jar:\
~/hbase/lib/junit-3.8.1.jar:\
~/hbase/lib/libthrift-r771587.jar:\
~/hbase/lib/log4j-1.2.15.jar:\
~/hbase/lib/lucene-core-2.2.0.jar:\
~/hbase/lib/servlet-api-2.5-6.1.14.jar:\
~/hbase/lib/xmlenc-0.52.jar:\
~/hbase/lib/zookeeper-3.2.2.jar:\
~/hbase/lib/jsp-2.1/jsp-2.1.jar:\
~/hbase/lib/jsp-2.1/jsp-api-2.1.jar

export HBASE_CLASSPATH=$CLASSPATH

export HBASE_HOME=$HOME/hbase

export JAVA_HOME=/opt/jdk1.6.0_20/

export PATH=$HBASE_HOME/bin:$PATH

$HBASE_HOME/bin/hbase regionserver start 

