#!/bin/bash

export CLASSPATH="\
/Users/ekoontz/hbase/conf:\
/Users/ekoontz/hbase/target/classes:\
/Users/ekoontz/hadoop-common/build/classes:\
/Users/ekoontz/hadoop-hdfs/build/classes:\
/Users/ekoontz/zookeeper/build/classes:\
/Users/ekoontz/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar:\
/Users/ekooontz/.m2/repository/commons-lang/commons-lang/2.5/commons-lang-2.5.jar:\
/Users/ekoontz/.m2/repository/commons-logging/commons-logging/1.1.1/commons-logging-1.1.1.jar:\
/Users/ekoontz/.m2/repository/log4j/log4j/1.2.15/log4j-1.2.15.jar:\
/Users/ekoontz/.m2/repository/org/mortbay/jetty/jetty/6.1.24/jetty-6.1.24.jar:\
/Users/ekoontz/.m2/repository/org/mortbay/jetty/jetty-util/6.1.24/jetty-util-6.1.24.jar:\
/Users/ekoontz/.m2/repository/javax/servlet/servlet-api/2.4/servlet-api-2.4.jar:\
/Users/ekoontz/.m2/repository/tomcat/jasper-compiler/5.5.23/jasper-compiler-5.5.23.jar:\
/Users/ekoontz/.m2/repository/tomcat/jasper-runtime/5.5.23/jasper-runtime-5.5.23.jar:\
/Users/ekoontz/.m2/repository/javax/servlet/jsp-api/2.0/jsp-api-2.0.jar:\
/Users/ekoontz/.m2/repository/org/mortbay/jetty/jsp-2.1/6.1.14/jsp-2.1-6.1.14.jar:\
/Users/ekoontz/.m2/repository/org/mortbay/jetty/jsp-api-2.1/6.1.14/jsp-api-2.1-6.1.14.jar:\
/Users/ekoontz/.m2/repository/commons-el/commons-el/1.0/commons-el-1.0.jar:\
/Users/ekoontz/hbase/target/classes:"

java org.apache.hadoop.hbase.master.HMaster start


