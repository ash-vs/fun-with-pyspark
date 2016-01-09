import pyspark as ps
import person as p

sc = ps.SparkContext()

people = [p.Person("Jon", "Galvanize"), p.Person("Ann", "Pearson"), p.Person("Matei", "Databricks")]

# Create an RDD from an in-memory list
person_rdd = sc.parallelize(people)

# Create a new RDD that gets executed lazily
companies_rdd = person_rdd.map(lambda x: x.company)

companies_list = companies_rdd.collect()
print companies_list
