import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (countDistinct,broadcast,year,quarter,upper,regexp_replace,lower,
                                   when,count,col,sum,trim,initcap,rlike)
from pyspark.sql.window import Windows
spark = SparkSession.builder \
    .appName("Practice") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark Version:", spark.version)

sales_df=spark.read.option("header",True)\
.option("inferSchema",True).csv("C:/Users/HP/OneDrive/Documents/Interview_prep/Pandas/sales.csv")
#sales_df.show(5)

customers_df=spark.read.option("header",True)\
.option("inferSchema",True).csv("C:/Users/HP/OneDrive/Documents/Interview_prep/Pandas/customers.csv")
#sales_df.show(5)

customers_df.printSchema()

customers_df=customers_df.withColumn("Name",initcap(trim("Name")))

"""Email_validation steps"""
"""
Invalid email count:
====================="""
email_regex=r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

invalid_count=customers_df.filter(~col("email").rlike(email_regex)|col("email").isNull()).count()
print(invalid_count)

customers_df=customers_df.withColumn("email", when(col("email").rlike(email_regex),lower(col("email"))).otherwise(None))
"""
null value count:
=================="""
invalid_count=customers_df.filter(col("email").isNull()).count()
print(invalid_count)

customers_df=customers_df.\
    withColumn("email_valid_flag",when(col("email").rlike(email_regex),"Valid").otherwise("Invalid"))
customers_df.show()
customers_df.groupby("email_valid_flag").count().show()



"""count of all column nulls"""
customers_df.select([count(when(col(c).isNull(),c)).alias(c)
                    for c in customers_df.columns]).show()

"Phone number check"
customers_df=customers_df.withColumn("phone",regexp_replace(col("phone"),"[^0-9]",""))
customers_df.show()
customers_df=customers_df.withColumn("phone",when(col("phone").between(10,15),col("phone")).otherwise(None))
"""Remove duplicate data"""
customers_df.dropDuplicates(["customer_id"])

customers_df.filter(col("name").isNotNull()& col("customer_id").isNotNull()).show()

"""Product_data"""
product_df=spark.read.option("header",True)\
.option("inferSchema",True).csv("C:/Users/HP/OneDrive/Documents/Interview_prep/Pandas/products.csv")
product_df.show(5)

product_df=product_df.withColumn('product_name',trim(col("product_name")))\
        .withColumn("SKU",trim(upper(col("SKU"))))
product_df.select("SKU").show()
reg_ex=r"^[A-Z0-9]{6,15}$"                
product_df=product_df.withColumn("SKU",when(col("SKU").rlike(reg_ex),col("SKU")).otherwise(None))
count1=product_df.filter(col("SKU").isNull()).count()
print("count",count1)
product_df.select("SKU").show()

payment_df=spark.read.option("header",True)\
.option("inferSchema",True).csv("C:/Users/HP/OneDrive/Documents/Interview_prep/Pandas/payments.csv")
payment_df.show(5)



#Standadized column Names
for c in sales_df.columns:
    print()
    sales_df=sales_df.withColumnRenamed(c,c.strip().upper().replace(" ","_"))
sales_df.printSchema()

#How t
# o identify count of Null values in each columns
sales_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in sales_df.columns
]).show()

#sales_df.select([sum(when(col(c).isNull(),1).otherwise(0))\
 #                   .alias(c)for c in sales_df.columns]).show()
sales_df=sales_df.fillna({"return_date":"2015-12-01"})
#sales_df.show()

sales_df=sales_df.dropDuplicates(["order_id","product_id"])

#sales by quarter

sales_df=sales_df.select("customer_id","sale_id","order_id","product_id","order_date","order_total","return_flag")

#sales_df=sales_df.filter(col("return_flag")=='No')

sales_df=sales_df.withColumn("year",year(col("order_date")))\
        .withColumn("quarter",quarter(col("order_date")))
sales_df.show()
gold_sales_by_quarter=sales_df.groupby("year","quarter")\
    .agg(sum("order_total").alias("totl_sales"))
gold_sales_by_quarter.show()

product_df=product_df.select("product_id","product_name")
sales_product_df=sales_df.join(broadcast(product_df),on="product_id",how="inner")
gold_sales_by_product=sales_product_df.groupby("product_id","product_name")\
.agg(sum("order_total").alias("toal_ales"))
gold_sales_by_product.show()

#sum the total order and returns for each customer and product

cust_product_summary_df=sales_df.groupby("customer_id","product_id")\
.agg(sum("order_total").alias("total"),
     sum(when(col("return_flag")=="Yes",col("order_total"))\
         .otherwise(0)).alias("return sales")
)
cust_product_summary_df.show()
cust_product_summary_df=cust_product_summary_df.withColumn('net_sales',col("total")-col("return sales"))
cust_product_summary_df.show()

sales_df.show()