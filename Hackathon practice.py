from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,coalesce,split,explode

spark=SparkSession.builder.appName("PharmaETLpipeline").getOrCreate()

df=spark.read.option("multiline","true").json("C:/Users/HP/OneDrive/Documents/Interview prep/Pandas/data.json")
df.show()
df_clean=df.withColumn("dosage",coalesce(col("dosage"),lit("UNKNOWN")))
df_clean.show()
df_array=df_clean.withColumn("drug_id",explode(split(col("drug_ids"),",")))
df_array.show()