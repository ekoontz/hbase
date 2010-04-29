/opt/jdk1.6.0_20/bin/java \
  -Xmx1000m \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:+UseConcMarkSweepGC \
  -XX:+CMSIncrementalMode \
  -Dhbase.log.dir=/home/ekoontz/stable/hbase-0.20.3/logs \
  -Dhbase.log.file=hbase.log \
  -Dhbase.home.dir=/home/ekoontz/stable/hbase-0.20.3 \
  -Dhbase.id.str= -Dhbase.root.logger=INFO,console \
  -Djava.library.path=/home/ekoontz/stable/hbase-0.20.3/lib/native/Linux-amd64-64 \
  -classpath \
/home/ekoontz/hbase/build/classes:\
/home/ekoontz/hbase/core/target/classes:\
/home/ekoontz/stable/hbase-0.20.3/conf:/opt/jdk1.6.0_20/lib/tools.jar:\
/home/ekoontz/stable/hbase-0.20.3:/home/ekoontz/stable/hbase-0.20.3/hbase-0.20.3.jar:\
/home/ekoontz/stable/hbase-0.20.3/conf:/opt/jdk1.6.0_20/lib/tools.jar:\
/home/ekoontz/stable/hbase-0.20.3:\
/home/ekoontz/stable/hbase-0.20.3/hbase-0.20.3-test.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/AgileJSON-2009-03-30.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/commons-cli-2.0-SNAPSHOT.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/commons-el-from-jetty-5.1.4.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/commons-httpclient-3.0.1.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/commons-logging-1.0.4.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/commons-logging-api-1.0.4.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/commons-math-1.1.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/hadoop-0.20.1-hdfs127-core.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/hadoop-0.20.1-test.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/jasper-compiler-5.5.12.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/jasper-runtime-5.5.12.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/jetty-6.1.14.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/jetty-util-6.1.14.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/jruby-complete-1.2.0.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/json.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/junit-3.8.1.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/libthrift-r771587.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/log4j-1.2.15.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/lucene-core-2.2.0.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/servlet-api-2.5-6.1.14.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/xmlenc-0.52.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/zookeeper-3.2.2.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/jsp-2.1/jsp-2.1.jar:\
/home/ekoontz/stable/hbase-0.20.3/lib/jsp-2.1/jsp-api-2.1.jar \
 org.apache.hadoop.hbase.master.HMaster start



