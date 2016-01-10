import pyspark as ps
import time
import numpy as np

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

# create a new RDD that turns each row into a dictionary
airline_rows_as_dicts_rdd = airline_no_header_rdd.map(make_row_dict)
check_list = airline_rows_as_dicts_rdd.take(2)
print "Data in key-value pairs:\n{0}".format(check_list)

# Let's now try to rank airports 
# in terms of arrival/departure delays

# In each row, the arr_delay corresponds to the dest_airport_id
# and the dep_delay corresponds to the origin_airport_id

# Let's transform our data to group delays with the corresponding
# airports rather than grouped by flight

# Extract tuples of the destination airports and arrival delays
# and return the delay value casted as a float for future computation
destination_airport_rdd = airline_rows_as_dicts_rdd.map(lambda row: \
	(row['DEST_AIRPORT_ID'], \
	float(row['ARR_DELAY']) if row['ARR_DELAY'] else 0))
check_list = destination_airport_rdd.take(2)
print "Destination airports and arrival delays:\n{0}".format(check_list)

# Extract tuples of the origin airports and departure delays
# and return the delay value casted as a float for future computation
origin_airport_rdd = airline_rows_as_dicts_rdd.map(lambda row: \
	(row['ORIGIN_AIRPORT_ID'], \
	float(row['DEP_DELAY']) if row['DEP_DELAY'] else 0))
check_list = origin_airport_rdd.take(2)
print "Origin airports and departure delays:\n{0}".format(check_list)

# Find the mean delay for departures and arrivals for each airport
mean_arrival_delay_by_destination_airport_rdd = \
	destination_airport_rdd.groupByKey().mapValues(lambda delays: np.mean(delays.data))
check_list = mean_arrival_delay_by_destination_airport_rdd.take(2)
print "Mean arrival delays by destination airport:\n{0}".format(check_list)

mean_departure_delay_by_origin_airport_rdd = \
	origin_airport_rdd.groupByKey().mapValues(lambda delays: np.mean(delays.data))
check_list = mean_arrival_delay_by_destination_airport_rdd.take(2)
print "Mean departure delays by origin airport:\n{0}".format(check_list)

# Find the highest-ranked and lowest-ranked airports by mean delay times
# Caveat - It would be more rigorous to take departure delays into account
# 		   when calculating arrival delays since the former affects the latter
destination_airports_sorted_by_mean_arrival_delay_rdd = \
	mean_arrival_delay_by_destination_airport_rdd.sortBy(lambda t: t[1], ascending=True)
check_list = destination_airports_sorted_by_mean_arrival_delay_rdd.take(10)
print "Ten highest-ranked airports by arrival delay:\n{0}".format(check_list) 

destination_airports_sorted_by_mean_arrival_delay_rdd = \
	mean_arrival_delay_by_destination_airport_rdd.sortBy(lambda t: t[1], ascending=False)
check_list = destination_airports_sorted_by_mean_arrival_delay_rdd.take(10)
print "Ten lowest-ranked airports by arrival delay:\n{0}".format(check_list) 

origin_airports_sorted_by_mean_departure_delay_rdd = \
	mean_departure_delay_by_origin_airport_rdd.sortBy(lambda t: t[1], ascending=True)
check_list = origin_airports_sorted_by_mean_departure_delay_rdd.take(10)
print "Ten highest-ranked airports by departure delay:\n{0}".format(check_list)

origin_airports_sorted_by_mean_departure_delay_rdd = \
	mean_departure_delay_by_origin_airport_rdd.sortBy(lambda t: t[1], ascending=False)
check_list = origin_airports_sorted_by_mean_departure_delay_rdd.take(10)
print "Ten lowest-ranked airports by departure delay:\n{0}".format(check_list)
