export CLASSPATH=\
~/hbase/build/classes:\
~/hbase/core/target/classes:\
~/stable/hbase-0.20.3/conf:/opt/jdk1.6.0_20/lib/tools.jar:\
~/stable/hbase-0.20.3:~/stable/hbase-0.20.3/hbase-0.20.3.jar:\
~/stable/hbase-0.20.3/conf:/opt/jdk1.6.0_20/lib/tools.jar:\
~/stable/hbase-0.20.3:\
~/stable/hbase-0.20.3/hbase-0.20.3-test.jar:\
~/stable/hbase-0.20.3/lib/AgileJSON-2009-03-30.jar:\
~/stable/hbase-0.20.3/lib/commons-cli-2.0-SNAPSHOT.jar:\
~/stable/hbase-0.20.3/lib/commons-el-from-jetty-5.1.4.jar:\
~/stable/hbase-0.20.3/lib/commons-httpclient-3.0.1.jar:\
~/stable/hbase-0.20.3/lib/commons-logging-1.0.4.jar:\
~/stable/hbase-0.20.3/lib/commons-logging-api-1.0.4.jar:\
~/stable/hbase-0.20.3/lib/commons-math-1.1.jar:\
~/stable/hbase-0.20.3/lib/hadoop-0.20.1-hdfs127-core.jar:\
~/stable/hbase-0.20.3/lib/hadoop-0.20.1-test.jar:\
~/stable/hbase-0.20.3/lib/jasper-compiler-5.5.12.jar:\
~/stable/hbase-0.20.3/lib/jasper-runtime-5.5.12.jar:\
~/stable/hbase-0.20.3/lib/jetty-6.1.14.jar:\
~/stable/hbase-0.20.3/lib/jetty-util-6.1.14.jar:\
~/stable/hbase-0.20.3/lib/jruby-complete-1.2.0.jar:\
~/stable/hbase-0.20.3/lib/json.jar:\
~/stable/hbase-0.20.3/lib/junit-3.8.1.jar:\
~/stable/hbase-0.20.3/lib/libthrift-r771587.jar:\
~/stable/hbase-0.20.3/lib/log4j-1.2.15.jar:\
~/stable/hbase-0.20.3/lib/lucene-core-2.2.0.jar:\
~/stable/hbase-0.20.3/lib/servlet-api-2.5-6.1.14.jar:\
~/stable/hbase-0.20.3/lib/xmlenc-0.52.jar:\
~/stable/hbase-0.20.3/lib/zookeeper-3.2.2.jar:\
~/stable/hbase-0.20.3/lib/jsp-2.1/jsp-2.1.jar:\
~/stable/hbase-0.20.3/lib/jsp-2.1/jsp-api-2.1.jar

export HBASE_CLASSPATH=$CLASSPATH

export HBASE_HOME=$HOME/hbase

export JAVA_HOME=/opt/jdk1.6.0_20/

export PATH=$HBASE_HOME/bin:$PATH

$HBASE_HOME/bin/hbase master start 

