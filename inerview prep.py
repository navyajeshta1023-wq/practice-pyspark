
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark=SparkSession.builder.appName('interviewprep').getOrCreate()
"""
id| name|
+---+-----+
|  1|Steve|
|  2|David|
|  3|Aryan|
+---+-----+
id|subject|marks|
+---+-------+-----+
|  1|pyspark|   90|
|  1|    sql|   80|
|  2|    sql|  100|
|  2|pyspark|   70|
|  3|pyspark|   40|
|  3|    sql|   90|
+---+-------+-----+
output tale:
=================
id| name|percentage|     Result|
+---+-----+----------+-----------+
|  1|Steve|      85.0|distinction|
|  2|David|      85.0|distinction|
|  3|Aryan|      65.0|First class|

data=[(1,"Steve"),(2,"David"),(3,"Aryan")]
columns=["id","name"]
st=spark.createDataFrame(data,columns)
st.show()
data=[(1,"pyspark",90),(1,"sql",80),(2,"sql",100),(2,"pyspark",70),(3,"pyspark",40),(3,"sql",90)]
columns=["id","subject","marks"]
st_marks=spark.createDataFrame(data,columns)
st_marks.show()
avg=st.join(st_marks,on="id",how="left")
avg=avg.groupBy("id","name").agg(F.avg("marks").alias("percentage"))
#avg.select("*",F.when(F.col("percentage")>=70,"distinction")).show()
avg.withColumn("Result",F.when(F.col("percentage")>=70,"distinction")\
                         .when(F.col("percentage")<70,"First class")\
                          .otherwise("fail")).show()
"""

"""
You have two tables.one is for customers and another is for order details.find all the 
customers with out any order

cust_data=[(1,"virat","delhi"),(2,"rohit","mumbai"),(3,"shami","kolkata"),
           (4,"bumrah","gujrat"),(5,"bhuvi","pune")]
cust_schema=["cust_id","name","city"]
cust_df=spark.createDataFrame(cust_data,cust_schema)
cust_df.show()
order_data=[(205,2),(218,3),(314,1),(165,2)]
order_schema=["order_id","cust_id"]
order_df=spark.createDataFrame(order_data,order_schema)
order_df.show()
result=cust_df.join(order_df,on="cust_id",how="left")
result.show()
result.filter(result.order_id.isNull()).select("cust_id").show()
"""

"""
Find the products whose total sales revenue has increased every year.
step1: first find previous sales column using lag
step2: then find flag 

product_id|year|total_sales_revenue|
+----------+----+-------------------+
|         1|2019|             1000.0|
|         1|2020|             1200.0|
|         1|2021|             1100.0|
|         2|2019|              500.0|
|         2|2020|              600.0|
|         2|2021|              900.0|
|         3|2019|              300.0|
|         3|2020|              450.0|
|         3|2021|              400.0|

product_data=[(1,"Laptop","Electronics"),(2,"Jeans","Clothing"),(3,"Chairs","Home")]
product_schema=["product_id","product_data","category"]
product_df=spark.createDataFrame(product_data,product_schema)
#product_df.show()
sales_data=[(1,2019,1000.00),(1,2020,1200.00),(1,2021,1100.00),(2,2019,500.00),
            (2,2020,600.00),(2,2021,900.00),(3,2019,300.00),(3,2020,450.00),
            (3,2021,400.00)]
sales_schema=["product_id","year","total_sales_revenue"]
sales_df=spark.createDataFrame(sales_data,sales_schema)
sales_df.show()
sales_df=sales_df.withColumn("previous_revenue",F.lag(F.col("total_sales_revenue"))\
                      .over(Window.partitionBy("product_id").orderBy(F.col("year"))))\
        .withColumn("flag",F.when(F.col("previous_revenue").isNull(),True)\
            .otherwise(F.col("total_Sales_revenue")>F.col("previous_revenue")))\


sales_df.show()  


sales_df=sales_df.groupBy("product_id").agg(F.min(F.col("flag")).alias("isincresin"))\
                .filter(F.col("isincresin")==True)

sales_df.show()
sales_df.join(product_df,on="product_id",how="inner").select("category").show()
"""

"""#write a spark qery to report the movie with an odd number id,description that is not boring
return the result in decresing order of rating.
movie_data=[(1,"war","great 3d",8.9),(22,"science","fiction",8.5),(3,"irish","interesting",6.5),
       (4,"Ice song","Fatacy",9.0),(5,"House card","boring",9.1)]
schema=["id","movie","description","rating"]
movie_schema=spark.createDataFrame(movie_data,schema)
movie_schema.show()
movie_schema.filter((F.col("id")%2!=0) & (F.col("description" )!="boring"))\
    .orderBy(F.desc("rating")).show()
    """
"""
name|  item|weight|
+-----+------+------+
|Rahul|Tomato|     3|
|Virat| Apple|     2|
|Rahul|Tomato|     1|
|Rahul| Apple|     3|
|Priya|Orange|     6|
|Virat| Apple|     1|
|Priya|Orange|     2|
output:
============
id    items
priya   [{"orange",8}]
Virat   [{"Virat",3}]
Rahul   [{"Tomato",4},{"App}]
"""
"""
fruit_Data=[("Rahul","Tomato",3),("Virat","Apple",2),("Rahul","Tomato",1),
            ("Rahul","Apple",3),("Priya","Orange",2),("Priya","Orange",6),("Virat","Apple",1)]
                
fruit_schema=["name", "item","weight"]
fruit_df=spark.createDataFrame(fruit_Data,fruit_schema)
fruit_df.show()
final=fruit_df.groupBy("name","item").agg(F.sum(F.col("weight")).alias("total_Weight"))
final.show()
from pyspark.sql.functions import collect_set,struct
df_final=final.groupBy("name").agg(collect_set(struct("item","total_weight")).alias("items"))
df_final.show()

"""
"""
#Get departmetwise highest slary
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
data = [
    (1, "IT", 50000),
    (2, "HR", 40000),
    (3, "IT", 55000),
    (4, "HR", 60000)
]
df = spark.createDataFrame(data, ["empid", "dept", "salary"])
df.show()
win=Window.partitionBy("dept").orderBy(F.col("salary").desc())
df=df.withColumn("rnk",row_number().over(win)).filter(F.col("rnk")==1)
df.show()
"""
"""
emp_data = [(1, "A", 101), (2, "B", 102), (3, "C", 103)]
mgr_data = [(101, "X"), (104, "Y")]

emp = spark.createDataFrame(emp_data, ["empid", "ename", "managerid"])
mgr = spark.createDataFrame(mgr_data, ["managerid", "mname"])

join_types = ["inner", "left", "right", "outer", "left_semi", "left_anti"]
#
#for j in join_types:
 ##  print(f"{j} join count = {count}")
emp.show()
mgr.show()

inner join count = 1
left join count = 3
right join count = 2
outer join count = 4
left_semi join count = 1
left_anti join count = 2
"""
"""
#Task: Total order amount per region for customers with more than one order.

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, count

spark = SparkSession.builder.getOrCreate()

orders_data = [
    (101, 1, 200),
    (102, 2, 500),
    (103, 1, 300),
    (104, 3, 700),
    (105, 3, 150),
    (106, 4, 400)
]

customers_data = [
    (1, "East"),
    (2, "West"),
    (3, "East"),
    (4, "North"),
    (5, "South")
]

orders = spark.createDataFrame(orders_data, ["order_id", "customer_id", "amount"])
customers = spark.createDataFrame(customers_data, ["customer_id", "region"])

order=orders.groupBy("customer_id").count().filter("count>1")
order.show()
orders.join(order,on="customer_id").join(customers,on="cstomer_id").groupby("region").agg(sum(F.col("salary")))
"""

"""

#Handling Duplicates
#Keep latest (empid, dept) record.
#steps:

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

employee_data = [
    (101, "HR", 40000, "2024-01-10"),
    (101, "HR", 45000, "2024-03-15"),
    (102, "IT", 60000, "2024-02-11"),
    (103, "IT", 55000, "2024-03-20"),
    (103, "IT", 52000, "2024-01-25")
]

emp = spark.createDataFrame(employee_data, ["empid", "dept", "salary", "updated_at"])
win=Window.partitionBy("empid","dept").orderBy(F.col("updated_at").desc())
emp_latest=emp.withColumn("rnk",row_number().over(win)).filter(col("rnk")==1).drop("rnk")
emp_latest.show()

"""


#Frequency of characters in a string


s="swiss"
d={}
for i in s:
    d[i]=d.get(i,0)+1
print(d)


