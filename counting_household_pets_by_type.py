import pyspark as ps

sc = ps.SparkContext()

pets = sc.parallelize([("cat", 1), ("dog", 1), ("cat", 2)])

# Unlike reduce, this first groups the values
# by key and then reduces them separately
# x is the accumulator
# y is the current value
reducedByKey = pets.reduceByKey(lambda x, y: x + y)
print "Reduced output: {0}".format(reducedByKey.collect())

# Unlike reduceByKey, groupByKey and sortByKey don't
# use accumulators to provide a single output value
groupedByKey = pets.groupByKey()
print "Grouped output: {0}".format(groupedByKey.collect())

sortedByKey = pets.sortByKey()
print "Sorted output: {0}".format(sortedByKey.collect())
