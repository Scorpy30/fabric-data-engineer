# Use Apache Spark in Microsoft Fabric Module - QnA

## Unit 1: Introduction - QnA

### Q: What is Apache?  
  A: Apache is a foundation that develops and maintains open-source software projects. Apache Spark is one of those projects — a framework for large-scale data processing.  

### Q: What is Spark?  
  A: Apache Spark is a distributed computing system designed to process huge datasets quickly. It can run tasks in parallel across many machines and supports in-memory computation, making it faster than older disk-based systems.  

### Q: What is Microsoft Fabric?  
  A: Microsoft Fabric is an integrated data platform that combines data engineering, data science, real-time analytics, and business intelligence in one environment. It lets you use Spark alongside other services like Lakehouse and Power BI without switching tools.  

### Q: What is Big Data?  
  A: Big Data refers to datasets that are too large or complex for traditional tools to handle efficiently. Think terabytes or petabytes of data coming from sources like sensors, logs, or social media.  

### Q: What are traditional frameworks Spark improves upon?  
  A: Before Spark, frameworks like Hadoop MapReduce were common. They processed data in batches and relied heavily on disk I/O, which made them slower. Spark’s in-memory processing and richer APIs (SQL, streaming, MLlib) are more efficient.  
  (Note: Pandas is a Python library for data analysis, but it’s not a distributed framework — it works well for smaller datasets, not true “big data.”)  

### Q: What is the difference between a framework and a library?  
  A: A **library** is a collection of functions/classes you call directly in your code (e.g., Pandas in Python). A **framework** is a larger structure that dictates how your code should be organized and run, often providing a runtime environment (e.g., Apache Spark, Hadoop). In short:  
    - Library → you call it.  
    - Framework → it calls you (your code fits into its structure).

### Q: Does this difference have to do with accessibility (Apache vs Pandas)?  
  A: Not exactly. Pandas is a Python-specific library, so you need Python to use it. Apache Spark is a framework that can be accessed from multiple languages (Python, Scala, Java, R) and runs across clusters of machines. The difference is more about **scope and scale** than accessibility.

### Q: Is Apache a company?  
  A: Apache is not a company — it’s the **Apache Software Foundation (ASF)**, a non-profit organization that maintains many open-source projects. Spark is one of those projects, just like Hadoop, Kafka, and many others.

### Q: So Apache Spark is one of their projects?  
  A: Yes. Apache Spark is an open-source project under the Apache Software Foundation. Hadoop is another one. Both are frameworks for big data processing, but Spark is newer and faster because it supports in-memory computation.

### Q: Is Hadoop also a framework?  
  A: Yes. Hadoop is an older big data framework that relies on disk-based batch processing (MapReduce). Spark is considered more efficient because it can process data in memory and supports richer APIs for SQL, streaming, and machine learning.

### Q: Why is Apache Spark considered more efficient than traditional data processing frameworks for big data?  
  A: Because it supports parallel processing across clusters and in-memory computation, which speeds up analytics compared to disk-based systems.

### Q: How does Microsoft Fabric’s integration of Spark differ from using Spark in standalone platforms like HDInsight or Synapse?  
  A: In Fabric, Spark runs in the same environment as other Fabric services (like Lakehouse, Data Warehouse, Power BI), reducing friction when combining Spark jobs with broader analytics workflows. 

### Q: What does "in-memory" vs "disk-based" mean in data processing?  
  A:  
  - **In-memory** → Data is loaded into RAM (fast, temporary storage) and processed directly there. This avoids repeated reads/writes to disk, making computations much faster. Spark uses this approach.  
  - **Disk-based** → Data is read from and written back to disk (hard drive/SSD) after each step. This is slower because disk I/O (input/output) takes more time. Hadoop MapReduce uses this approach.

### Q: Why is in-memory faster?  
  A: RAM access is measured in nanoseconds, while disk access is measured in milliseconds. That difference (thousands of times faster) makes iterative algorithms and analytics much more efficient in Spark.

### Q: Does in-memory mean Spark can’t handle huge datasets?  
  A: No. Spark can spill data back to disk if RAM isn’t enough, but its default strategy is to keep as much as possible in memory for speed. It’s designed to scale across clusters of machines.

### Q: Is Pandas disk-based or in-memory?  
  A: Pandas is **in-memory**, but only on a single machine. It loads data into your computer’s RAM. The limitation is that Pandas can’t scale beyond one machine’s memory, while Spark distributes data across many machines.

---

## Unit 2: Prepare to use Apache Spark

### Q: What is a Spark pool?  
  A: A cluster of compute nodes in Fabric where Spark jobs run. Includes a head node (driver) and worker nodes (executors).

### Q: Why do we need pools?  
  A: Pools distribute tasks across machines, making big data processing scalable and fast.

### Q: What’s the difference between starter pool and custom pool?  
  A: Starter pool is pre-configured for quick use. Custom pools let you choose node types, autoscaling, and executor allocation.

### Q: What is a Spark runtime?  
  A: A runtime defines the version of Spark, Delta Lake, Python, and libraries available in your environment.

### Q: What is a custom environment?  
  A: A workspace setup where you specify runtime, libraries, Spark pool, and configs for specialized workloads.

### Q: What is the native execution engine?  
  A: A Fabric optimization that runs Spark operations directly on Lakehouse infra, faster for large Parquet/Delta datasets.

### Q: What is high concurrency mode?  
  A: A mode that lets multiple users share Spark sessions efficiently while keeping their code isolated.

### Q: What is MLFlow logging?  
  A: An automatic logging system for machine learning experiments, tracking training and deployment without extra code.  

### Q: What is Spark administration for Fabric capacity?  
  A: Managing Spark pools at capacity level (permissions, usage, DR).

### Q: What is a Lakehouse?
  A: A Lakehouse is a hybrid data architecture that combines:  
  1. Data Lake → stores raw, unstructured, semi‑structured data (like JSON, CSV, Parquet).  
  2. Data Warehouse → optimized for structured, relational queries (SQL).  
  The Lakehouse gives you both flexibility and performance: you can store all types of data in one place, but still query it efficiently with SQL engines.  

### Q: What is OneLake?
  A: OneLake is Microsoft Fabric’s unified data lake.  
  It’s like the “OneDrive for data” — a single storage system where all Fabric workloads (Spark, Data Warehouse, Power BI, Real‑Time Analytics) can access the same data without duplication.  
  It supports open formats like Parquet and Delta Lake, so Spark jobs and SQL queries can work on the same files.

### Q: Why did my first Spark job take 2 minutes to run?”  
  A: Starter pool cold start; subsequent runs are faster.

### Q: Where do I run %%configure code?  
  A: In the first cell of a Fabric notebook.

---

## Unit 3: Run Spark Code – QnA

### Q: What are the two main ways to run Spark in Fabric?
  A: You can run Spark interactively in notebooks or as automated Spark job definitions.

### Q: When should I use a notebook?
  A: Use notebooks for exploration, analysis, and learning. They let you mix text, images, and code in cells, and results appear inline immediately.

### Q: When should I use a Spark job definition?
  A: Use job definitions for repeatable, automated pipelines. They run scripts on‑demand or on a schedule, and are best for production ETL tasks.

### Q: Where do I put %%configure code?
  A: In the first cell of a notebook. Job definitions don’t support notebook magics — they rely on script configuration.

### Q: What’s the difference in output between notebooks and job definitions?
  A: Notebooks show results inline (tables, charts). Job definitions produce logs and update data in the Lakehouse.

### Q: Can both notebooks and job definitions access the Lakehouse?
  A: Yes. Both can attach to a Lakehouse, query data with Spark SQL or PySpark, and write results back.

### Q: Why did my first notebook cell take 2 minutes to run?
  A: That’s the starter pool cold start. The pool spins up nodes the first time. Subsequent runs are faster because the pool stays warm.

### Q: Can I schedule notebooks like job definitions?
  A: No. Notebooks are interactive only. For scheduling, you must use a Spark job definition.

---

## Unit 4: Work with Data in a Spark DataFrame – QnA

### Q: What is a DataFrame in Spark?
  A: A distributed collection of data organized into named columns, similar to Pandas but optimized for big data.

### Q: How is a DataFrame different from an RDD?
  A: RDD is the low-level resilient distributed dataset. DataFrames add schema and SQL-like operations, making them easier and faster for structured data.

### Q: Why specify an explicit schema?
  A: It ensures correct column types, avoids inference errors, and improves performance.

### Q: How do I filter rows in a DataFrame?
  A: Use `.where()` or `.filter()` methods with conditions, e.g. `df.where(df["Category"] == "Mountain Bikes")`.

### Q: What format is best for saving DataFrames?
  A: Parquet — it’s efficient, columnar, and widely supported.

### Q: What is partitioning and why use it?
  A: Partitioning splits data into folders by column values, improving query performance by reducing unnecessary reads.

### Q: Can I load only one partition of data?
  A: Yes. You can specify the partition path (e.g., `Category=Road Bikes`) when reading.

---

## Unit 5: Work with data using Spark SQL - QnA

### Q: What is Spark SQL?
  A: A Spark library that lets you query structured data using SQL syntax. It integrates with the DataFrame API.

### Q: What is the Spark catalog?
  A: A metastore that holds relational objects (views, tables). It enables you to query DataFrames with SQL and manage persistent tables.

### Q: What is the difference between a temporary view and a table?
  A: Temporary views exist only for the current session and disappear when it ends. Tables are persisted in the catalog and stored in the Lakehouse.

### Q: What is a managed table vs an external table?
  A: Managed tables store data in the Lakehouse Tables area; deleting them removes the data. External tables only store metadata in the catalog; data remains in external storage, so deleting them doesn’t remove the files.

### Q: Why is Delta format preferred in Fabric?
  A: Delta tables support transactions, versioning, and streaming data. They combine the reliability of relational databases with the scalability of big data.

### Q: How do you query a DataFrame with SQL?
  A: Register it as a view (`df.createOrReplaceTempView`) and then run SQL queries using `spark.sql()` or `%%sql` in notebooks.

### Q: Can you partition Delta tables?
  A: Yes. Partitioning improves performance by reducing the amount of data scanned during queries.

### Q: What happens if you delete a managed table vs an external table?
  A: Deleting a managed table removes both metadata and underlying data. Deleting an external table removes only metadata; the data files remain intact.

### Q: What is delta table?
  A: A Delta table in Spark (and Microsoft Fabric) is a special type of table built on the Delta Lake format. It’s designed to bring the reliability of relational databases into big data systems.  
  **What Makes a Delta Table Different**  
  1. Transactional Storage  : Delta tables support ACID transactions (Atomicity, Consistency, Isolation, Durability). This means multiple users can read/write safely without corrupting data.  

  2. Versioning & Time Travel : Every change to a Delta table is logged. You can query older versions of the table to “time travel” and reproduce past results.  
  
  3. Schema Enforcement & Evolution : Delta tables enforce column types (so you don’t accidentally insert wrong data). They also support schema evolution — you can add new columns without breaking existing queries.  
  
  4. Performance Optimizations : Delta tables store data in Parquet format but add metadata layers for faster queries, indexing, and partition pruning.
  
  5. Streaming + Batch Together : Delta tables unify streaming and batch processing. You can continuously append data (like logs or IoT events) and still query it with SQL.

---

## Unit 6: Visualize data in a Spark notebook - QnA

### Q: Why do we need visualizations in Spark notebooks?
  A: Visualizations help interpret large datasets by revealing patterns and trends that are not obvious in raw tables.

### Q: How can you visualize DataFrames directly in Fabric notebooks?
  A: Use `display(df)` to show a table, then switch to chart view (bar, line, pie) using the notebook UI.

### Q: Can Spark SQL queries be visualized?
  A: Yes. Run queries with `spark.sql()` or `%%sql`, then use the notebook’s chart options to visualize the results.

### Q: Which Python libraries are commonly used for visualization in Spark notebooks?
  A: Matplotlib, Seaborn, and Plotly are popular choices for creating static and interactive charts.

### Q: How do you visualize time series data?
  A: Use line charts to plot values over time, often after aggregating data by date or month.

### Q: How can notebook visualizations be shared in Fabric?
  A: Pin charts to Fabric dashboards, which can be shared with teams and updated automatically when the notebook reruns.