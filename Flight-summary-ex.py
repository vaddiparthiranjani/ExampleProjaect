from pyspark.sql import SparkSession
from pyspark.sql import *

spark = SparkSession.builder.appName('DF_to_dict').getOrCreate()

flightSummary = spark.read.csv("../pythonProject/flight-summary.csv", header =True)

flightSummary.select(count("origin_city"),countDistinct("origin_city")).show()

#flightSummary.select(count("origin_city"),countDistinct("origin_city"),approx_count_distinct("origin_city",0.001)).show();
