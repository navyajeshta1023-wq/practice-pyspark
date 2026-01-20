"""import sys, os
print("Python version:", sys.version)
print("Driver:", os.environ.get("PYSPARK_DRIVER_PYTHON"))
print("Worker:", os.environ.get("PYSPARK_PYTHON"))
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Test").getOrCreate()
"""Calculate total spend per customer.
For each customer, check if the spending shows an increasing trend across dates. Output: cust_id, is_increasing (True/False).
data = [
    (1, "2025-09-01", 100),
    (1, "2025-09-02", 200),
    (2, "2025-09-01", 300),
    (2, "2025-09-02", 200),
    (3, "2025-09-01", 400),
    (3, "2025-09-02", 600),
]
columns = ["cust_id", "date", "amount"]
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("Employee_salary", IntegerType(), True)
])


df_manual = spark.createDataFrame(data, columns)
df_manual.show()

df_manual=df_manual.withColumn("prev_amount",F.lag("amount")\
                     .over(Window.partitionBy("cust_id").orderBy("date")))\
         .withColumn("isincreasing",F.when(F.col("prev_amount").isNull(),True)\
                     .otherwise(F.col("amount")>F.col("prev_amount")))

df=df_manual.groupBy("cust_id").agg(F.min(F.col("isincreasing")).alias("isincresing"))
df.show()
df_manual.select("cust_id","isincreasing").filter(F.col("isincreasing")==False).show()
"""

"""
Join to get emp_name, dept_name.

Count number of employees in each department.

emp_data = [
    (1, "Navya", 101),
    (2, "Amit", 102),
    (3, "Priya", 101),
]
emp_cols = ["emp_id", "emp_name", "dept_id"]
emp=spark.createDataFrame(emp_data,emp_cols)

dept_data = [
    (101, "HR"),
    (102, "Finance"),
]
dept_cols = ["dept_id", "dept_name"]
dept=spark.createDataFrame(dept_data,dept_cols)
emp.show()
dept.show()

emp.join(dept,on="dept_id",how="left")\
    .groupBy("dept_name").agg(F.count(F.col("emp_name"))).alias("Emp_count").show()
    """
"""#Explode:
#==================
data = [
    ("A", ["Python", "SQL","Python"]),
    ("B", ["Java", "Spark"]),
    ("C", ["Python", "AWS"]),
]
columns = ["emp_name", "skills"]

df=spark.createDataFrame(data,columns)
Exploaded_Data=df.withColumn("skill",F.explode("skills"))
Exploaded_Data.show()

Exploaded_Data.groupBy("emp_name")\
               .pivot("skill")\
               .agg(F.count("skill"))\
               .fillna(0).show()
"""
"""
#How to find duplicate records based on specific columns from dataframe.
df.show()
df.groupBy("emp_id").count().filter("count==1").show()
"""
#pyspark having dropDuplicates randomly but it doesnot care about
#  latest occurence recoed
#So we are using window function

data = [
    (1, "A", "2024-08-01 10:00:00"),
    (1, "A", "2024-08-02 15:00:00"),  # later record for id=1
    (2, "B", "2024-09-01 09:00:00"),
    (2, "B", "2024-10-03 12:00:00"),  # later record for id=2
    (3, "C", "2024-05-05 14:30:00"),
    (3, "C", "2024-08-05 14:30:00"),  # duplicate timestamp for id=3
]

columns = ["id", "name", "timestamp"]
df=spark.createDataFrame(data,columns)
#df.show()
"""
#Removing dplicate records based on latest recoed 
win=Window.partitionBy("id").orderBy(F.desc("timestamp"))
df.withColumn("rnk",F.row_number().over(win)).filter("rnk=1")\
    .select("id","name","timestamp").show()
"""
#Extract year & month from a date column and count employees joined per year-month.
df.withColumn("year",F.year("timestamp"))\
  .withColumn("month",F.month("timestamp"))\
  .groupBy("year","month").count().show()