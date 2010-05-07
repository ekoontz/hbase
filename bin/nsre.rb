#!./bin/hbase shell

include Java

$LOAD_PATH.unshift File.dirname($PROGRAM_NAME)
require 'HBase'

puts "nsre.rb start.."

myadmin = HBaseAdmin.new(org.apache.hadoop.hbase.HBaseConfiguration.new())

puts myadmin.tableExists 'people'
puts myadmin.tableExists 'peoplex'
status = myadmin.getClusterStatus()

for server in status.getServerInfo()
  # server is a : ...hbase.HServerInfo object
  puts "regions for : " + server.toString()
  for region in server.getLoad().getRegionsLoad()
    # region is a : ...hbase.RegionLoad object. (see 
    puts("REGION: %s" % [ region.toString() ])

    # get a regionInfo from this regionLoad object.
  end

end

puts "nsrb.end."
exit

