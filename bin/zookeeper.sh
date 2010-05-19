#!/bin/sh 

export CLASSPATH=/home/ekoontz/hbase/core/target/classes

export HBASE_CLASSPATH=$CLASSPATH

export HBASE_HOME=/home/ekoontz/hbase

export JAVA_HOME=/opt/jdk1.6.0_20/

export PATH=/home/ekoontz/hbase/bin:$PATH

hbase zookeeper start 


