import pyspark as ps
import random

sc = ps.SparkContext()

flips = 1000000

# lazy evaluation via a Python generator
coins = xrange(flips)

# lazy evaluation again when creating our RDD, so
# none of the transformations are executed until 
# the count() action is executed and materializes 
# the result
coins_rdd = sc.parallelize(coins)
flips_rdd = coins_rdd.map(lambda i: random.random())
heads_rdd = flips_rdd.filter(lambda r: r < 0.51)
heads =  heads_rdd.count()
print "Flips that were heads: {0}".format(heads)

# Could chain that all like this:
# heads = sc.parallelize(coins) \
# 			.map(lambda i: random.random()) \
# 			.filter(lambda r: r < 0.51) \
# 			.count()
# print heads