# Demo: Working with Data using Spark SQL

## 1. Create Temporary View
```python
df.createOrReplaceTempView("products_view")
```

## 2. Save as Delta Table
```python
df.write.format("delta").saveAsTable("products")
```

## 3. Create External Table
```python
spark.catalog.createExternalTable(
    "external_products",
    path="Files/data/products.csv",
    source="csv",
    options={"header":"true"}
)
```

## 4. Query with Spark SQL API
```python
bikes_df = spark.sql(
    "SELECT ProductID, ProductName, ListPrice \
     FROM products \
     WHERE Category IN ('Mountain Bikes','Road Bikes')"
)
display(bikes_df)
```

## 5. Query with %%sql Magic
```python
%%sql
SELECT Category, COUNT(ProductID) AS ProductCount
FROM products
GROUP BY Category
ORDER BY Category
```

## 6. Partitioned Delta Table
```python
df.write.partitionBy("Category").format("delta").mode("overwrite").saveAsTable("partitioned_products")
```