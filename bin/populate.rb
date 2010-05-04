#!/usr/bin/jruby

include Java

$LOAD_PATH.unshift File.dirname($PROGRAM_NAME)
require 'HBase'
require 'Formatter'

@configuration = org.apache.hadoop.hbase.HBaseConfiguration.new()
@configuration.setInt("hbase.client.retries.number", 7)
@configuration.setInt("ipc.client.connect.max.retries", 3)
format_width = 110
@formatter = Formatter::Console.new(:format_width => format_width)

def admin()
  @admin = HBase::Admin.new(@configuration, @formatter) unless @admin
  @admin
end

puts "populate.rb start.."

admin().create('people','preferences')
admin().disable('people')
admin().alter('people',{'NAME' => 'location'})
admin().enable('people')

people = HBase::Table.new(@configuration, 'people', @formatter)

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

1000.times {
  surname = surnames.slice(rand(surnames.size))
  given_name = given_names.slice(rand(given_names.size))
  name = given_name + " " + surname
  fruit = fruits.slice(rand(fruits.size))
  vegetable = fruits.slice(rand(vegetables.size))
  people.put(name,'preferences:fruit',fruit)
  people.put(name,'preferences:vegetable',vegetable)
  people.put(name,'location:zip',(rand(99999)).to_s)
}
