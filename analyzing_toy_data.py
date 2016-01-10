import pyspark as ps
import json

sc = ps.SparkContext()

# create an RDD from a local file containing data for analysis
# Note: either use the file protocol or an absolute path to 
# 		avoid having Spark think you're trying to pull data
#		from a distributed file store such as HDFS 
file_rdd = sc.textFile('/Users/ashsrinivas/workspace/fun-with-pyspark/bulk_data/toy_data.txt')

# Check out the first element just to see some data
first_item = file_rdd.first()
print "First item only: {0}".format(first_item)

# Check out some more data
first_items = file_rdd.take(3)
print "First several items: {0}".format(first_items)

# Can do this for a small amount of data
all_items = file_rdd.collect()
print "All items: {0}".format(all_items)

# Find all of the users who have purchased more
# than 5 items from the toy store (note all data is in json)
json_rdd = file_rdd.map(lambda x: json.loads(x))

# First convert the dictionary into a tuple with the number of
# purchases converted from a string to a number since reduceByKey
# cannot accept a dictionary, and we need the number of items 
# purchased in numeric format for later operations
mapped_big_spenders_rdd = json_rdd.map(lambda x: tuple((x.keys()[0], int(x.values()[0]))))
print "Mapped big spender output:\n{0}".format(mapped_big_spenders_rdd.collect())

# Reduce the transformed RDD of big spenders to get their combined purchases by name
reduced_big_spenders_rdd = mapped_big_spenders_rdd.reduceByKey(lambda a, b: a + b)
print "Reduced big spender output:\n{0}".format(reduced_big_spenders_rdd.collect())

# Finally, filter the reduced by key RDD of big spenders 
# by only keeping those who purchased more than 5 items
filtered_big_spenders_rdd = reduced_big_spenders_rdd.filter(lambda x: x[1] > 5)
print "Filtered big spender output:\n{0}".format(filtered_big_spenders_rdd.collect())

formatted_list = ["{0} bought {1} toys".format(x[0], x[1]) \
				 for x in filtered_big_spenders_rdd.collect()]

for i in formatted_list:
	print i
