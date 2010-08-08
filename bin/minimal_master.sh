#!/bin/bash

export CLASSPATH="\
/home/ekoontz/hbase/conf:\
/home/ekoontz/hbase/target/classes:\
/home/ekoontz/hadoop-common/build/classes:\
/home/ekoontz/hadoop-hdfs/build/classes:\
/home/ekoontz/zookeeper/build/classes:\
/home/ekoontz/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar:\
/home/ekooontz/.m2/repository/commons-lang/commons-lang/2.5/commons-lang-2.5.jar:\
/home/ekoontz/.m2/repository/commons-logging/commons-logging/1.1.1/commons-logging-1.1.1.jar:\
/home/ekoontz/.m2/repository/log4j/log4j/1.2.15/log4j-1.2.15.jar:\
/home/ekoontz/.m2/repository/org/mortbay/jetty/jetty/6.1.24/jetty-6.1.24.jar:\
/home/ekoontz/.m2/repository/org/mortbay/jetty/jetty-util/6.1.24/jetty-util-6.1.24.jar:\
/home/ekoontz/.m2/repository/javax/servlet/servlet-api/2.4/servlet-api-2.4.jar:\
/home/ekoontz/.m2/repository/tomcat/jasper-compiler/5.5.23/jasper-compiler-5.5.23.jar:\
/home/ekoontz/.m2/repository/tomcat/jasper-runtime/5.5.23/jasper-runtime-5.5.23.jar:\
/home/ekoontz/.m2/repository/javax/servlet/jsp-api/2.0/jsp-api-2.0.jar:\
/home/ekoontz/.m2/repository/org/mortbay/jetty/jsp-2.1/6.1.14/jsp-2.1-6.1.14.jar:\
/home/ekoontz/.m2/repository/org/mortbay/jetty/jsp-api-2.1/6.1.14/jsp-api-2.1-6.1.14.jar:\
/home/ekoontz/.m2/repository/commons-el/commons-el/1.0/commons-el-1.0.jar:\
/home/ekoontz/hbase/target"

java org.apache.hadoop.hbase.master.HMaster start


