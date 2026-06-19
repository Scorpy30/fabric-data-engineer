# Use Apache Spark in Microsoft Fabric Module

## Unit 1: Introduction

- Apache Spark is an open-source distributed-parallel processing framework for large-scale data(big data) processing and analytics.   
- Developed under the Apache Software Foundation.  
- Spark is widely used in "big data" scenarios and is available in multiple platforms, including Azure HDInsight, Azure Synapse Analytics, and Microsoft Fabric.  
- Microsoft Fabric integrates Spark with other services like Lakehouse and Power BI.   
- Spark is faster than older frameworks like Hadoop MapReduce because it supports in-memory computation.  
- This module focuses on using Spark in Microsoft Fabric to ingest, process, and analyze data in a lakehouse.   
- While Spark techniques are common across implementations, Microsoft Fabric integrates Spark with other data services, making it easier to build end-to-end analytics solutions.   
- Big Data = datasets too large/complex for traditional single-machine tools (like Pandas).  
- In-memory processing → Spark keeps data in RAM for faster computation.  
- Disk-based processing → Hadoop MapReduce writes intermediate results to disk, slower due to I/O.  
- Spark can spill to disk if RAM is insufficient, but prefers memory.  
- Pandas also works in-memory, but only on one machine (not distributed).  

---

## Unit 2: Prepare to use Apache Spark

- Spark is a distributed data processing framework that uses a "divide and conquer" approach across multiple nodes in a cluster.   
- In Microsoft Fabric, these clusters are called **Spark pools**.

### Spark Pools
A Spark pool consists of compute nodes that distribute data processing tasks.  
![Spark_Pool_Architecture](./demo/screenshots/sparkpool.png)

- Head node → runs driver program, coordinates tasks.  
- Worker nodes → executors that perform actual processing.  
- Supports Java, Scala, R, SQL, PySpark.  
- Can process data stored in **OneLake Lakehouse**.

### Spark Pools in MS Fabric
Fabric provides a starter pool in each workspace for quick setup.  
You can manage settings in the Admin portal under **Capacity settings → Data Engineering/Science Settings**.  

👉 For a **Configuration Example**, see [Spark Pools Setup](./demo/spark-pool.md#1-spark-pool-creation-window)

Pool configuration settings include:
- Node family (VM type, usually memory-optimized)  
- Autoscale (auto-provision nodes)  
- Dynamic allocation (adjust executors based on data volume)  

### Runtime and Environments
- **1. Spark runtimes in Microsoft Fabric**  
  Define versions of Spark, Delta Lake, Python, and libraries. Multiple runtimes supported.  

- **2. Environments in Microsoft Fabric**  
  Custom environments allow specific runtimes, libraries (PyPI/custom), Spark pools, and overrides.  

### Additional Spark Configuration Options
- **1. Native execution engine** → vectorized processing directly on Lakehouse infra, faster for Parquet/Delta.  
- **2. High concurrency mode** → share Spark sessions across multiple users safely.  
- **3. Automatic MLFlow logging** → logs ML experiments automatically.  
- **4. Spark administration for a Fabric capacity** → manage Spark pools at capacity level (permissions, usage, DR, surge protection).  

👉 [See demo for code snippet](./demo/spark-pool.md#4-native-execution-engine-experiment-code)

### Spark Pool Architecture (Text Diagram)

                ┌───────────────────────────────┐
                │        Spark Pool             │
                └───────────────────────────────┘
                           │
                           ▼
        ┌───────────────────────────────────────────┐
        │                 Head Node                 │
        │   - Runs Driver Program                   │
        │   - Coordinates tasks across workers      │
        └───────────────────────────────────────────┘
                           │
                           ▼
        ┌───────────────────────────────────────────┐
        │               Worker Nodes                │
        │   - Executors run tasks                   │
        │   - Store data in memory (RAM)            │
        │   - Communicate with OneLake storage      │
        └───────────────────────────────────────────┘
                           │
                           ▼
        ┌───────────────────────────────────────────┐
        │             OneLake Lakehouse             │
        │   - Unified storage for structured +      │
        │     unstructured data                     │
        │   - Accessible by Spark SQL, PySpark, etc.│
        └───────────────────────────────────────────┘

---

## Unit 3: Run Spark Jobs

### Running Spark in Fabric
- Two main options:
  - **Notebooks** → interactive, cell‑based, immediate results.
  - **Spark Job Definitions** → automated, repeatable, can be scheduled.

### Notebooks

When you want to use Spark to explore and analyze data interactively, use a notebook.  
- Combine text, images, and code (Python, Scala, R, SQL).
- Organized into **cells** (markdown or executable code).
- Results appear inline after execution.
- Best for exploration, learning, and analysis.

**Example:**
```python
data = [("Tarush", 1), ("Copilot", 2)]
df = spark.createDataFrame(data, ["Name", "Value"])
df.show()
```
👉 Check out [Sales Analytics Notebook](./demo/notebooks/Sales_Analytics.ipynb)  
👉 Short notes [Click here](./demo/run-spark-code.md#-fabric-data-engineer-notebook-sales-order-exploration)

### Spark Job Definitions

If you want to use Spark to ingest and transform data as part of an automated process, you can define a Spark job to run a script on-demand or based on a schedule.

- Define a script to run on‑demand or on a schedule.  
- Can reference external files (e.g., Python helper libraries).  
- Attach to a specific Lakehouse for data access.  
- Best for production pipelines and repeatable transformations.  

#### Example Setup:
- Script: transform.py
- References: utils.py for helper functions
- Lakehouse: Lakehouse_1
- Schedule: Daily at 9 AM

👉 Short notes [Click here](./demo/run-spark-code.md#️-spark-job-definition-automated-etl--sales-transform)

### Key Differences
| Feature | Notebook | Spark Job Definition |
| --- | --- | --- |
| Purpose | Interactive exploration | Automated pipelines |
| Execution | Manual, cell‑by‑cell | Scheduled or on‑demand |
| Output | Inline results | Logs + Lakehouse updates |
| Best Use Case | Learning, prototyping | Production ETL, batch jobs |