from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("Files/orders/2019.csv", header=True, inferSchema=True)

# Example transformation: calculate total revenue
df = df.withColumn("Revenue", df["Quantity"] * df["UnitPrice"])
agg = df.groupBy("Item").sum("Revenue")
agg.show()
