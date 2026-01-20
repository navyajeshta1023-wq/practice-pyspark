from pyspark.sql import SparkSession
#spark=SparkSession.builder.appName('Interview').getOrCreate()
spark = SparkSession.builder.appName("Interview").getOrCreate()

data = [
    ("A", "HR", 50000),
    ("B", "IT", 60000),
    ("C", "Finance", 55000),
    ("D", "IT", 70000),
    ("E", "HR", None),
    ("F", "Finance", 65000),
]
columns = ["emp_name", "department", "salary"]

df = spark.createDataFrame(data, columns)
df.show()