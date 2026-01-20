
#print(spark.version)
"""
Create a DataFrame.
Fill missing salary with department-wise average salary.
Find the highest salary employee per department using window functions.

#df=spark.read.csv(r"C:\Users\HP\OneDrive\Documents\Interview prep\Pandas\employeenew.csv",header=True,inferSchema=True)
df.show()

Avg=df.groupBy("department").agg(F.avg(F.col("Employee_salary")).alias("Averagesalary"))
Avg.show()
df.join(Avg,on="department",how="left")\
.withColumn("Employee_salary",
            F.when(F.col("Employee_salary").isNull(),F.col("Averagesalary"))\
            .otherwise(F.col("Employee_salary")))
df.show()
#Find the highest salary employee per department using window functions.
from pyspark.sql.window import Window
win=Window.partitionBy("department")\
    .orderBy(F.col("Employee_Salary").desc())
df.withColumn("Rank",F.row_number().over(win)).filter(F.col("Rank")==1).show()
"""
#C:\Users\HP\OneDrive\Documents\Interview prep\Pandas\employee.csv
#df.select("emp_id","name").show()
#print(df.columns)
#print(df.schema.names)
#print(df.dtypes)
#df.filter(df.salary>30000).show()
#Add new column with 10% of salary.
#from pyspark.sql.functions import col
#df=df.withColumn("bonus",col("salary")*0.1)
#df.show()

#Rename a column
#df=df.withColumnRenamed("salary","Employee_salary")
#df.show()

#Drop a column
#df=df.drop("bonus")
#df.show()
#Top 3 Salaries in Company (Global, No Window)
#from pyspark.sql import functions as F
#df.orderBy(F.col("Employee_salary").desc()).limit(3).show()

"""
#Group by department and find average salary
from pyspark.sql import functions as F
Average=df.groupBy("department")\
    .agg(F.avg("Employee_salary").alias("Average_salary"))


#check employees salary is greater than dept avg salary  using withcolum
result=df.join(Average,on="department")\
.withColumn("Stmt",F.col("Employee_salary")>col("Average_salary"))
result.show() 
"""

"""#get employees whose salary is greater than department average salary.
Required_Emp=df.join(Average,on="department")\
.filter(F.col("Employee_salary")>F.col("Average_salary"))\
.select("emp_id","department")
Required_Emp.show()  
"""
"""
from pyspark.sql.window import Window                                                                              
from pyspark.sql import functions as f
win=Window.partitionBy("department")
df.withColumn("AverageSalary",F.avg("Employee_salary").over(win))\
.filter(F.col("Employee_salary")>F.col("AverageSalary"))\
.select("emp_id","department").show()
"""
"""
from pyspark.sql.window import .

from pyspark.sql import functions as F

win = Window.partitionBy("department")

df.withColumn("avg_salary", F.avg("Employee_salary").over(win))
"""

"""# Top 3 salaries per Department
 
from pyspark.sql.window import Window
from pyspark.sql import functions as F
df.withColumn("Rnk",F.row_number().over(Window.partitionBy("department")\
        .orderBy(F.desc("Employee_salary")))).filter(F.col("Rnk")==1).show()
"""

#from pyspark.sql.functions import when,approx_count_distinct
#Display Distinct departments
#df.select("department").distinct().show()

#count number of distinct valuues in Column

#cont=df.select("department").distinct().count()
#print(cont)

#Count of distinct rows on DataFrame
#dist_count=df.distinct().count()
#print(dist_count)

#unique=df.agg(approx_count_distinct("emp_id")).collect()[0][0]
#print(unique)

#Adding new column with condition  (if else condition)
#df.withColumn("Bonus",when(df.Employee_salary>30000,df.Employee_salary*0.3).otherwise(df.Employee_salary*0.1)).show()

#Idetifying number of missing values in a column
#na=df.filter(col("department").isNull()).count()
#print(na)

"""Are you able to update Column values in pyspark
Yes, but dataframes are immutable so we dont do inplace manipulations.
So create new dataframe using when and otherwise 
"""
#df=df.withColumn("department",when(col("department")=="HR",None)\
#              .otherwise(col("department")))

#Idetifying number of missing values in a column
#na=df.filter(col("department").isNull()).count()
#print(na)

#How to fill null values with random value
#df.fillna({"department":"HR"}).show()

"""Pyspark Optimization
use cache() /persist() when reusing dataframes
use repartition()/coalesce() wisely.
Filter early before joins
"""
""" Fill missing values with mean per department.
To calculate this first find mean per department then join with original table
from pyspark.sql import functions as F
Mean_Table=df.groupBy("department").agg(F.avg("Employee_salary").alias("AverageSalary"))
Mean_Table.show()

df=df.join(Mean_Table,on="department",how="left")\
.withColumn("Employee_salary",
            F.when(F.col("Employee_salary").isNull(),F.col("AverageSalary"))\
                .otherwise(F.col("Employee_salary")))
df.select("Emp_id","department","Employee_Salary").show()
"""
"""
# Try to do same with window
from pyspark.sql.window import Window
from pyspark.sql import functions as F

df.withColumn("AverageSalary",F.mean("Employee_salary")\
                .over( Window.partitionBy("department"))).show()

#How to get number of partitions for a dataframe
print(df.rdd.getNumPartitions())
"""