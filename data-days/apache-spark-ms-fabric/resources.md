🏗️ Apache Spark Architecture
Driver Program → The “brain” of Spark. It defines the application logic and coordinates tasks.

Cluster Manager → Allocates resources across the cluster (examples: YARN, Kubernetes, or Spark’s built-in manager).

Executors → Worker processes running on cluster nodes. They execute tasks assigned by the driver and store data in memory.

Resilient Distributed Dataset (RDD) → Spark’s core data structure. It represents distributed collections of data across nodes, enabling parallel operations.

High-level APIs:

Spark SQL → Structured queries.

Spark Streaming → Real-time data streams.

MLlib → Machine learning.

GraphX → Graph analytics.

🏗️ Hadoop Architecture
HDFS (Hadoop Distributed File System) → Stores massive datasets across multiple machines with replication for fault tolerance.

MapReduce → Programming model for processing data in parallel.

Map phase → Splits input into key-value pairs.

Reduce phase → Aggregates results.

YARN (Yet Another Resource Negotiator) → Resource manager that schedules jobs across the cluster.

🔑 Key Difference
Hadoop MapReduce → Disk-based, batch-oriented. Each step writes intermediate results to disk.

Spark → In-memory, iterative, faster. Keeps data in RAM when possible, reducing disk I/O.

📂 Where to Store This
Since this is reference knowledge (not directly from the module), it fits better in your resources.md later.
But if you want to capture your doubts and answers about “architecture” right now, you can also add a section in qna.md like:

## Architecture Doubts
- Q: What is the architecture of Apache Spark?
- A: Driver, Cluster Manager, Executors, RDD, APIs (SQL, Streaming, MLlib, GraphX).
- Q: What is the architecture of Hadoop?
- A: HDFS, MapReduce, YARN.
- Q: Key difference?
- A: Spark = in-memory, faster. Hadoop = disk-based, slower

# OneLake Lakehouse Architecture

                ┌───────────────────────────────┐
                │          OneLake              │
                │  Unified Data Lake            │
                │  - Stores all data (raw +     │
                │    structured) in open formats│
                └───────────────────────────────┘
                           │
                           ▼
        ┌───────────────────────────────────────────┐
        │              Lakehouse Layer              │
        │  - Combines Data Lake + Data Warehouse    │
        │  - Supports SQL queries + ML workloads    │
        │  - Delta/Parquet files for performance    │
        └───────────────────────────────────────────┘
                           │
                           ▼
        ┌───────────────────────────────────────────┐
        │         Fabric Services Access            │
        │  - Spark Pools (PySpark, SQL)             │
        │  - Data Warehouse (T-SQL)                 │
        │  - Power BI (visualization)               │
        │  - Real-Time Analytics                    │
        └───────────────────────────────────────────┘
