from pyspark.sql import SparkSession
from pyspark.sql import *

spark = SparkSession.builder.appName('DF_to_dict').getOrCreate()

employee = spark.read.csv("../pythonProject/employee.csv", header =True)
department = spark.read.csv("../pythonproject/department.csv", header = True)
employee.show()
department.show()

# Left semi join
# employee.join(department, employee.dept_no == department.id,"left_semi").\
# select("first_name","dept_no").show();

# left-anti join
# employee.join(department,employee.col("dept_no").equalTo(department.col("id")),"left_anti").show();

# cross join
employee.crossJoin(department).show();

# Inner Join

# employee.join(department,employee.col("dept_no").equalTo(department.col("id")),"inner")
# .select("first_name","dept_no").show();

# Left-outer join

# employee.join(department,employee.col("dept_no").equalTo(department.col("id")),"left_outer").show();

# right-outer join
# employee.join(department,employee.col("dept_no").equalTo(department.col("id")),"right_outer").show();

# outer-join
# employee.join(department,employee.col("dept_no").equalTo(department.col("id")),"outer").show();




