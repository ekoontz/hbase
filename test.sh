#!/bin/sh
set -x

/opt/jdk1.6.0_20/jre/bin/java -Xmx512m \
 -Dbuild.test=/home/ekoontz/hbase/build/test \
 -Dsrc.testdata=/home/ekoontz/hbase/src/testdata \
 -Duser.dir=/home/ekoontz/hbase/build/test/data \
 -classpath /home/ekoontz/hbase/src/test:/home/ekoontz/hbase/build/test:\
/home/ekoontz/hbase/lib/AgileJSON-2009-03-30.jar:/home/ekoontz/hbase/lib/commons-cli-2.0-SNAPSHOT.jar:\
/home/ekoontz/hbase/lib/commons-el-from-jetty-5.1.4.jar:/home/ekoontz/hbase/lib/commons-httpclient-3.0.1.jar:\
/home/ekoontz/hbase/lib/commons-logging-1.0.4.jar:/home/ekoontz/hbase/lib/commons-logging-api-1.0.4.jar:\
/home/ekoontz/hbase/lib/commons-math-1.1.jar:/home/ekoontz/hbase/lib/hadoop-0.20.1-hdfs127-core.jar:\
/home/ekoontz/hbase/lib/hadoop-0.20.1-test.jar:/home/ekoontz/hbase/lib/jasper-compiler-5.5.12.jar:\
/home/ekoontz/hbase/lib/jasper-runtime-5.5.12.jar:/home/ekoontz/hbase/lib/jetty-6.1.14.jar:\
/home/ekoontz/hbase/lib/jetty-util-6.1.14.jar:/home/ekoontz/hbase/lib/jruby-complete-1.2.0.jar:\
/home/ekoontz/hbase/lib/json.jar:/home/ekoontz/hbase/lib/junit-3.8.1.jar:\
/home/ekoontz/hbase/lib/libthrift-r771587.jar:/home/ekoontz/hbase/lib/log4j-1.2.15.jar:\
/home/ekoontz/hbase/lib/lucene-core-2.2.0.jar:/home/ekoontz/hbase/lib/servlet-api-2.5-6.1.14.jar:\
/home/ekoontz/hbase/lib/xmlenc-0.52.jar:/home/ekoontz/hbase/lib/zookeeper-3.2.2.jar:\
/home/ekoontz/hbase/lib/jsp-2.1/jsp-2.1.jar:/home/ekoontz/hbase/lib/jsp-2.1/jsp-api-2.1.jar:\
/home/ekoontz/hbase/build/classes:/home/ekoontz/hbase/conf:\
/home/ekoontz/hbase/build:/usr/share/java/ant-launcher-1.7.1.jar:/usr/share/java/ant-1.7.1.jar:\
/usr/share/java/ant-junit-1.7.1.jar \
org.apache.tools.ant.taskdefs.optional.junit.JUnitTestRunner \
org.apache.hadoop.hbase.master.TestNSRE showoutput=true
