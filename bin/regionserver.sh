#!/bin/sh

set -x

/opt/jdk1.6.0_20/bin/java \
  -Xmx1000m \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:+UseConcMarkSweepGC \
  -XX:+CMSIncrementalMode \
  -Dhbase.log.dir=/home/ekoontz/hbase/logs \
  -Dhbase.log.file=hbase.log \
  -Dhbase.home.dir=/home/ekoontz/hbase \
  -Dhbase.id.str= -Dhbase.root.logger=INFO,console \
  -Djava.library.path=/home/ekoontz/hbase/lib/native/Linux-amd64-64 \
  -classpath \
/home/ekoontz/hbase/bin/../conf:\
/opt/jdk1.6.0_20/lib/tools.jar:\
/home/ekoontz/.m2/repository/ant/ant/1.6.5/ant-1.6.5.jar:\
/home/ekoontz/.m2/repository/asm/asm/3.1/asm-3.1.jar:\
/home/ekoontz/.m2/repository/com/google/protobuf/protobuf-java/2.3.0/protobuf-java-2.3.0.jar:\
/home/ekoontz/.m2/repository/com/sun/jersey/jersey-core/1.1.5.1/jersey-core-1.1.5.1.jar:\
/home/ekoontz/.m2/repository/com/sun/jersey/jersey-json/1.1.5.1/jersey-json-1.1.5.1.jar:\
/home/ekoontz/.m2/repository/com/sun/jersey/jersey-server/1.1.5.1/jersey-server-1.1.5.1.jar:\
/home/ekoontz/.m2/repository/com/sun/xml/bind/jaxb-impl/2.1.12/jaxb-impl-2.1.12.jar:\
/home/ekoontz/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar:\
/home/ekoontz/.m2/repository/commons-codec/commons-codec/1.2/commons-codec-1.2.jar:\
/home/ekoontz/.m2/repository/commons-el/commons-el/1.0/commons-el-1.0.jar:\
/home/ekoontz/.m2/repository/commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar:\
/home/ekoontz/.m2/repository/commons-lang/commons-lang/2.5/commons-lang-2.5.jar:\
/home/ekoontz/.m2/repository/commons-logging/commons-logging/1.1.1/commons-logging-1.1.1.jar:\
/home/ekoontz/.m2/repository/commons-net/commons-net/1.4.1/commons-net-1.4.1.jar:\
/home/ekoontz/.m2/repository/javax/activation/activation/1.1/activation-1.1.jar:\
/home/ekoontz/.m2/repository/javax/servlet/servlet-api/2.5/servlet-api-2.5.jar:\
/home/ekoontz/.m2/repository/javax/ws/rs/jsr311-api/1.1.1/jsr311-api-1.1.1.jar:\
/home/ekoontz/.m2/repository/javax/xml/bind/jaxb-api/2.1/jaxb-api-2.1.jar:\
/home/ekoontz/.m2/repository/javax/xml/stream/stax-api/1.0-2/stax-api-1.0-2.jar:\
/home/ekoontz/.m2/repository/jline/jline/0.9.94/jline-0.9.94.jar:\
/home/ekoontz/.m2/repository/junit/junit/4.8.1/junit-4.8.1.jar:\
/home/ekoontz/.m2/repository/log4j/log4j/1.2.15/log4j-1.2.15.jar:\
/home/ekoontz/.m2/repository/net/java/dev/jets3t/jets3t/0.6.1/jets3t-0.6.1.jar:\
/home/ekoontz/.m2/repository/org/apache/ant/ant/1.7.0/ant-1.7.0.jar:\
/home/ekoontz/.m2/repository/org/apache/ant/ant-launcher/1.7.0/ant-launcher-1.7.0.jar:\
/home/ekoontz/.m2/repository/org/apache/commons/commons-math/2.1/commons-math-2.1.jar:\
/home/ekoontz/.m2/repository/org/apache/hadoop/hadoop-core/0.20.2-with-200-826/hadoop-core-0.20.2-with-200-826.jar:\
/home/ekoontz/.m2/repository/org/apache/hadoop/hadoop-test/0.20.2-with-200-826/hadoop-test-0.20.2-with-200-826.jar:\
/home/ekoontz/.m2/repository/org/apache/hadoop/zookeeper/3.3.1/zookeeper-3.3.1.jar:\
/home/ekoontz/.m2/repository/org/apache/thrift/thrift/0.2.0/thrift-0.2.0.jar:\
/home/ekoontz/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.1.1/jackson-core-asl-1.1.1.jar:\
/home/ekoontz/.m2/repository/org/codehaus/jettison/jettison/1.1/jettison-1.1.jar:\
/home/ekoontz/.m2/repository/org/eclipse/jdt/core/3.1.1/core-3.1.1.jar:\
/home/ekoontz/.m2/repository/org/jruby/jruby-complete/1.4.0/jruby-complete-1.4.0.jar:\
/home/ekoontz/.m2/repository/org/mortbay/jetty/jetty/6.1.14/jetty-6.1.14.jar:\
/home/ekoontz/.m2/repository/org/mortbay/jetty/jetty-util/6.1.14/jetty-util-6.1.14.jar:\
/home/ekoontz/.m2/repository/org/mortbay/jetty/jsp-2.1/6.1.14/jsp-2.1-6.1.14.jar:\
/home/ekoontz/.m2/repository/org/mortbay/jetty/jsp-api-2.1/6.1.14/jsp-api-2.1-6.1.14.jar:\
/home/ekoontz/.m2/repository/org/mortbay/jetty/servlet-api-2.5/6.1.14/servlet-api-2.5-6.1.14.jar:\
/home/ekoontz/.m2/repository/org/slf4j/slf4j-api/1.5.8/slf4j-api-1.5.8.jar:\
/home/ekoontz/.m2/repository/org/slf4j/slf4j-log4j12/1.5.8/slf4j-log4j12-1.5.8.jar:\
/home/ekoontz/.m2/repository/oro/oro/2.0.8/oro-2.0.8.jar:\
/home/ekoontz/.m2/repository/stax/stax-api/1.0.1/stax-api-1.0.1.jar:\
/home/ekoontz/.m2/repository/tomcat/jasper-runtime/5.5.23/jasper-runtime-5.5.23.jar:\
/home/ekoontz/.m2/repository/xmlenc/xmlenc/0.52/xmlenc-0.52.jar:\
/home/ekoontz/hbase/bin/../core/target/classes:/home/ekoontz/hbase/bin/../core/target/test-classes:\
/home/ekoontz/hbase/bin/../lib/libthrift-0.2.0.jar \
 org.apache.hadoop.hbase.regionserver.HRegionServer start



