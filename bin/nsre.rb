#!./bin/hbase shell

include Java

$LOAD_PATH.unshift File.dirname($PROGRAM_NAME)
require 'HBase'

puts "nsre.rb start.."

myadmin = HBaseAdmin.new(org.apache.hadoop.hbase.HBaseConfiguration.new())

puts myadmin.tableExists 'people'
puts myadmin.tableExists 'peoplex'
status = myadmin.getClusterStatus()
puts status.getHBaseVersion()
puts status.getServerNames()[0]

puts "nsrb.end."
exit

