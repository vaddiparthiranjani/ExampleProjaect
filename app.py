from pyspark.sql import SparkSession
from pyspark.sql import *

spark = SparkSession.builder.appName('DF_to_dict').getOrCreate()

employee = spark.read.csv("../pythonProject/employee.csv", header =True)
department = spark.read.csv("../pythonproject/department.csv", header = True)
employee.show()
department.show()

#employee.join(department, employee.dept_no == department.id,"left_semi").\
  #            select("first_name","dept_no").show();
employee.crossJoin(department).show();

