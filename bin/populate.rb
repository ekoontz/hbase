#!./bin/hbase shell

# <much of this setup taken from hirb.rb.>
include Java

# Some goodies for hirb. Should these be left up to the user's discretion?
require 'irb/completion'

format_width = 110
log_level = org.apache.log4j.Level::ERROR

# Set logging level to avoid verboseness
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(log_level)

# Require HBase now after setting log levels
require 'hbase'

# Load hbase shell
require 'shell'

# Require formatter
require 'shell/formatter'

@formatter = Shell::Formatter::Console.new(:format_width => format_width)

puts "populate.rb start.."

create('people','preferences')

disable('people')

alter 'people',{'NAME' => 'location'}

enable('people')

hbase = Hbase::Hbase.new

# give Hbase a chance to get ready for population..
sleep 5

people = hbase.table('people',@formatter)    

surnames = ['smith',
            'johnson',
            'williams',
            'jones',
            'brown',
            'davis',
            'miller',
            'wilson',
            'moore',
            'taylor',
            'anderson',
            'taylor',
            'thomas',
            'hernandez',
            'moore',
            'martin',
            'jackson',
            'thompson',
            'white',
            'lopez',
            'lee',
            'gonzalez',
            'harris',
            'clark',
            'lewis',
            'robinson',
            'walker',
            'perez',
            'hall',
            'young',
            'allen',
            'sanchez',
            'wright',
            'king',
            'scott',
            'green',
            'baker',
            'adams',
            'nelson',
            'hill',
            'ramirez',
            'campbell',
            'mitchell',
            'roberts',
            'carter',
            'phillips']

given_names = [
               'james',
               'john',
               'robert',
               'michael',
               'william',
               'david',
               'richard',
               'charles',
               'joseph',
               'thomas',
               'christopher',
               'daniel',
               'paul',
               'mark',
               'donald',
               'george',
               'kenneth',
               'steven',
               'edward',
               'brian',
               'ronald',
               'anthony',
               'kevin',
               'jason',
               'matthew',
               'gary',
               'timothy',
               'jose',
               'larry',
               'jeffrey',
               'frank',
               'scott',
               'eric',
               'stephen',
               'andrew',
               'mary',
               'patricia',
               'linda',
               'barbara',
               'elizabeth',
               'jennifer',
               'maria',
               'susan',
               'margaret',
               'dorothy',
               'lisa',
               'nancy',
               'karen',
               'betty',
               'helen',
               'sandra'
]

fruits = [
          'apple',
          'apricot',
          'banana',
          'blackberry',
          'blueberry',
          'cantaloupe',
          'cherry',
          'cranberry',
          'date',
          'grape',
          'guava',
          'kiwi',
          'lychee',
          'mango',
          'pomegranate',
          'raspberry',
          'strawberry'
]

vegetables = [
              'asparagus',
              'bean',
              'beet',
              'broccoli',
              'squash'
]

8000.times {
  # surname: as with Spanish names, both mother's and father's surnames.
  surname = surnames.slice(rand(surnames.size)) + " " + 
            surnames.slice(rand(surnames.size));
  # given name: first name plus a middle name.
  given_name = given_names.slice(rand(given_names.size)) + " " + 
               given_names.slice(rand(given_names.size))
  full_name = given_name + " " + surname
  fruit = fruits.slice(rand(fruits.size))
  vegetable = vegetables.slice(rand(vegetables.size))
  people.put(full_name,'preferences:fruit',fruit)
  people.put(full_name,'preferences:vegetable',vegetable)
  people.put(full_name,'location:zip',(rand(99999)).to_s)
}
