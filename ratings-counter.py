# import boiler plate packages for Spark
from pyspark import SparkConf, SparkContext
import collections

# run on the local machine single threat single cpu
# app name 
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# spark context object needs to be created with the given configurations above
sc = SparkContext(conf = conf)

# reading line by line the text file
lines = sc.textFile("./ml-100k/u.data")

# execute a function with a map function
# and collects the ratingns into a new RDD
ratings = lines.map(lambda x: x.split()[2])
# value counts in the form of tuples
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
