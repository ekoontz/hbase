#regenerate master webpage:
rm hbase-server/target/classes/org/apache/hadoop/hbase/tmpl/master/MasterStatusTmpl.class \
   hbase-server/target/generated-jamon/org/apache/hadoop/hbase/tmpl/master/MasterStatusTmpl.java 
mvn compile
find . -name MasterStatusTmpl.java -ls -or -name MasterStatusTmpl.class -ls -or -name MasterStatusTmpl.jamon -ls
bin/hbase-daemon.sh restart master
