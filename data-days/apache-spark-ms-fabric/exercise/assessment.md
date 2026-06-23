# Assessment Questions

### 1. Your organization wants to minimize costs while ensuring adequate performance for sporadic high-load data processing tasks. Which Spark pool configuration should you implement in Microsoft Fabric?

A. Enable autoscale with a minimum of 1 node and a maximum based on peak load requirements.  
B. Use a fixed number of memory optimized nodes.  
C. Disable autoscale and dynamic allocation to maintain consistent node usage.  

**Answer:** A. Enable autoscale with a minimum of 1 node and a maximum based on peak load requirements.

---

### 2. You have a dataframe containing sales data and want to quickly visualize the total sales per category using built-in functionality in Microsoft Fabric notebooks. Which feature should you use?

A. Seaborn library  
B. Built-in notebook charts  
C. Matplotlib library  

**Answer:** B. Built-in notebook charts

---

### 3. In Microsoft Fabric, what is the purpose of setting a default Spark pool in a workspace?

A. To limit the number of Spark jobs that can run concurrently in the workspace.  
B. To automatically assign a Spark pool to jobs when no specific pool is specified.  
C. To enforce the use of memory optimized nodes for all tasks.  

**Answer:** B. To automatically assign a Spark pool to jobs when no specific pool is specified.

---

### 4. While working with Spark SQL in Microsoft Fabric, you need to create a temporary view from a dataframe for your analysis. Which command should you use to achieve this?

A. `df.select("column_name")`  
B. `df.createOrReplaceTempView("view_name")`  
C. `df.write.format("delta").saveAsTable("table_name")`  

**Answer:** B. `df.createOrReplaceTempView("view_name")`

---

### 5. When working with large datasets, which Spark DataFrame method is preferred for performance when only a subset of columns is needed?

A. `groupBy()`  
B. `filter()`  
C. `select()`  

**Answer:** C. `select()`

---

### 6. Which SQL magic command should you use in a Microsoft Fabric notebook to execute SQL queries directly?

A. `%%spark`  
B. `%%pyspark`  
C. `%%sql`  

**Answer:** C. `%%sql`

---

### 7. You have a dataframe with a column named `Date` in a string format, and you need to convert it to a date format for further analysis. Which PySpark method would you use?

A. `filter()`  
B. `selectExpr()`  
C. `withColumn()`  

**Answer:** C. `withColumn()`

---

### 8. You need to calculate the average sales amount for each product category in a Spark dataframe. Which code snippet accurately achieves this task?

A.
```python
avg_sales_df = df.groupBy('Category').agg({'SalesAmount': 'avg'})
```

B.
```python
avg_sales_df = df.agg({'SalesAmount': 'mean'}).groupBy('Category')
```

C.
```python
avg_sales_df = df.select('Category', 'avg(SalesAmount)')
```

**Answer:** A.

```python
avg_sales_df = df.groupBy('Category').agg({'SalesAmount': 'avg'})
```

---

### 9. What is a best practice when configuring Spark pools in Microsoft Fabric to handle varying data volumes efficiently?

A. Enable autoscale to adjust nodes based on data volume.  
B. Use a static number of nodes regardless of data volume.  
C. Disable dynamic allocation to ensure consistent node usage.  

**Answer:** A. Enable autoscale to adjust nodes based on data volume.

---