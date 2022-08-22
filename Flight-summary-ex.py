from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('DF_to_dict').getOrCreate()

flightSummary = spark.read.csv("../pythonProject/flight-summary.csv", header =True, inferSchema = True)

#flightSummary.select(count("origin_city"),countDistinct("origin_city")).show()

# flightSummary.select(count("origin_city"),countDistinct("origin_city"),approx_count_distinct("origin_city",0.001)).show()

# flightSummary.printSchema();

# flightSummary.select(min("count"),max("count")).show();

# flightSummary.select(sum("count"),sumDistinct("count")).show();
# flightSummary.select(avg("count")).show()
# flightSummary.select(skewness("count"),kurtosis("count")).show()

# flightSummary.select(variance("count").alias("variance"),stddev("count").alias("stddev")).show();
# flightSummary.groupBy("origin_airport").count().show(5)
# flightSummary.groupBy("origin_state","origin_city").count().where("origin_state = 'CA' ").show(5)
flightSummary.groupBy("origin_airport").agg(count("count").alias("Count"),
                                                                   min("count"),
                                                                   max("count"),
                                                                 sum("count")).show(5);




