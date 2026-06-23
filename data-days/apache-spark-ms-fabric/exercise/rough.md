# Assessment Questions

1. Your organization wants to minimize costs while ensuring adequate performance for sporadic high-load data processing tasks. Which Spark pool configuration should you implement in Microsoft Fabric? 
Enable autoscale with a minimum of 1 node and a maximum based on peak load requirements.
Correct
Use a fixed number of memory optimized nodes.
Disable autoscale and dynamic allocation to maintain consistent node usage.
2. You have a dataframe containing sales data and want to quickly visualize the total sales per category using built-in functionality in Microsoft Fabric notebooks. Which feature should you use? 
Seaborn library
Built-in notebook charts
Correct
Matplotlib library
3. In Microsoft Fabric, what is the purpose of setting a default Spark pool in a workspace? 
To limit the number of Spark jobs that can run concurrently in the workspace.
To automatically assign a Spark pool to jobs when no specific pool is specified.
Correct
To enforce the use of memory optimized nodes for all tasks.
4. While working with Spark SQL in Microsoft Fabric, you need to create a temporary view from a dataframe for your analysis. Which command should you use to achieve this? 
df.select("column_name")
df.createOrReplaceTempView("view_name")
Correct
df.write.format("delta").saveAsTable("table_name")
5. When working with large datasets, which Spark Dataframe method is preferred for performance when only a subset of columns is needed? 
groupBy
filter
select
Correct
6. Which SQL magic command should you use in a Microsoft Fabric notebook to execute SQL queries directly? 
%%spark
%%pyspark
%%sql
Correct
7. You have a dataframe with a column named 'Date' in a string format, and you need to convert it to a date format for further analysis. Which PySpark method would you use? 
filter
selectExpr
Incorrect
withColumn
8. You need to calculate the average sales amount for each product category in a Spark dataframe. Which code snippet accurately achieves this task? 
avg_sales_df = df.groupBy('Category').agg({'SalesAmount': 'avg'})
avg_sales_df = df.agg({'SalesAmount': 'mean'}).groupBy('Category')
avg_sales_df = df.select('Category', 'avg(SalesAmount)')
Incorrect
9. What is a best practice when configuring Spark pools in Microsoft Fabric to handle varying data volumes efficiently? 
Enable autoscale to adjust nodes based on data volume.
Correct
Use a static number of nodes regardless of data volume.
Disable dynamic allocation to ensure consistent node usage.