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

# Save the headers before stripping them off of the data
header_line = airline_no_quote_rdd.first()
header_line_list = header_line.split(',')
print "Headers:\n{0}".format(header_line_list)

# Now strip the headers off of our data by applying
# a filter that removes any row of data that isn't
# equal to the header_line we extracted earlier 
airline_no_header_rdd = airline_no_quote_rdd.filter(lambda row: row != header_line)
check_list = airline_no_header_rdd.take(2)
print "Data without headers:\n{0}".format(check_list)

def make_row_dict(row):
	# split each line's column values by splitting the string by comma 
	row_line_list = row.split(',')
	
	# create a dictionary using the header as keys 
	# and row items as values by doing the following:
	# use the zip() function to generate key-value tuples
	# use the tuples created by zip() to create a dictionary
	row_dict = dict(zip(header_line_list, row_line_list))
	return row_dict
	
airline_rows_as_dicts_rdd = airline_no_header_rdd.map(make_row_dict)
check_list = airline_rows_as_dicts_rdd.take(2)
print "Data in key-value pairs:\n{0}".format(check_list)

