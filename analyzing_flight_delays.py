import pyspark as ps
import time

sc = ps.SparkContext()

# load all of the airline data, which is quite large
data_location = '/Users/ashsrinivas/workspace/fun-with-pyspark/bulk_data/airline-data'
airline_rdd = sc.textFile(data_location)

# peek at the data
first_rows = airline_rdd.take(2)
print 'First rows:\n{0}'.format(first_rows)

# let's clean up the quotes and trailing comma
# we'll use replace to remove the quotes, and 
# the strip() method to remove the trailing comma
airline_no_quote_rdd = airline_rdd.map(lambda line: \
	line.replace('\'', '').replace('\"', '').strip(','))

# We can time operations 
start_time = time.time()

airline_no_quote_rdd.take(2)

end_time = time.time()

# print out the time it took to transform our data
print end_time - start_time

# We can cache data over a network if it's coming over the network
airline_no_quote_rdd.cache()
results = airline_no_quote_rdd.take(2)

start_time = time.time()

results = airline_no_quote_rdd.take(2)

end_time = time.time()

print "Time elapsed: {0} seconds".format(end_time - start_time)
print results
