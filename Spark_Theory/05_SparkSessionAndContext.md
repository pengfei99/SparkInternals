# Deep dive to spark session and context

## 1. What is spark context

`Prior to Spark 2.0`, **Spark Context** was the entry point of the spark applications. It allows users to
execute RDDs operations which calls low-level API to interact with Spark engine. For other operations, such as
`SQL, hive, etc`, we need to create `SQLContext, HiveContext, etc`. As a result, users need to manage multiple spark
entry point

## 2. What is spark session?

`Since Spark 2.0`, **Spark Session** has been introduced, which unified entry point to Spark (SQL, Hive, etc.).
A **Spark Session** contains:

- **SparkContext**: Core engine interface (RDD, cluster control)
- **SQLContext**  : Abstract interface for structured queries (merged in)
- **HiveSessionStateBuilder**: Enables Hive support if activated
- **Catalog**: Metadata access (DBs, tables, functions)
- **SessionState**  : Query execution pipeline
- **SharedState**: Cluster-wide shared catalog, configs
- **UDFRegistry**: Manages Python/Scala/SQL user-defined functions
- **QueryExecution**: Logical/physical plan + optimizer (Catalyst)
- **Configuration** : Runtime configs (spark.conf.get/set)
- **Extensions** : Hooks for injecting custom optimizers, strategies

With one **Spark Session**, a user can run DataFrames, SQL, Streaming, UDFs operations.

### 2.1 spark.sparkContext

The `spark context` inside a spark session `connects to cluster manager (local/YARN/k8s), manages job scheduling, DAGs, and RDD operations`.

It exposes the below functions:

- parallelize()
- broadcast()
- accumulator()
- statusTracker()
- .hadoopConfiguration() for HDFS access

### spark.conf 

During spark session creation, spark allows users to set up custom configurations:

Below is a complete example on how to set up config first, then create a spark session.
```python
from pyspark.sql import SparkSession
from pyspark import SparkConf

# create a spark conf
conf = SparkConf()

conf.set("spark.master", "local[*]")  # Use all available cores
conf.set("spark.app.name", "OptimizedLocalSparkApp")

# MEMORY
conf.set("spark.driver.memory", "4g")                   # Heap memory
conf.set("spark.driver.memoryOverhead", "1024")          # Off-heap for native libs

# GC TUNING
conf.set("spark.executor.extraJavaOptions",
         "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent")

# OFF-HEAP (if using Arrow, Parquet, etc.)
conf.set("spark.memory.offHeap.enabled", "true")
conf.set("spark.memory.offHeap.size", "1g")

# SERIALIZATION
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# SHUFFLE OPTIMIZATION
conf.set("spark.shuffle.file.buffer", "1m")
conf.set("spark.reducer.maxSizeInFlight", "96m")
conf.set("spark.shuffle.io.preferDirectBufs", "true")

# PYTHON CONFIG
conf.set("spark.python.worker.memory", "2g")
conf.set("spark.pyspark.python", "C:/Users/PLIU/Documents/git/SparkInternals/si_venv/Scripts/python.exe")  # Replace with your Python path
conf.set("spark.pyspark.driver.python", "C:/Users/PLIU/Documents/git/SparkInternals/si_venv/Scripts/python.exe")

# OPTIONAL: avoid memory leak from large broadcast variables
conf.set("spark.cleaner.referenceTracking.blocking", "true")

spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

```python
spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.set("spark.sql.adaptive.enabled", "true")

```

### 2.2 Hive support

In a spark session, we can enable Hive Support (activated via `.enableHiveSupport()`).
The hive support enables:
- Access to Hive catalogs
- Hive SQL dialect
- Hive UDFs
- SerDe support
- spark.sql.catalogImplementation = hive

> This option requires hive-site.xml, metastore-site.xml, and Hive warehouse config

### 2.3 spark.catalog

It manages the metadata Catalog Access (e.g. tables, views, databases, and functions)

For example, below functions are the most popular ones:
```python
spark.catalog.listDatabases()
spark.catalog.listTables("default")
spark.catalog.listFunctions()
```

### 2.4 spark.udf

User define function (UDF) Registry allows users to register `Python/Scala functions` as `SQL/UDF callable`

```python
from pyspark.sql.functions import udf
spark.udf.register("my_udf", lambda x: x + 1)

```

### QueryExecution (internal, debugging/optimization)
Access via:

```python
df._jdf.queryExecution().toString()

```
Shows:
- Logical Plan
- Optimized Logical Plan
- Physical Plan
- Result schema

### SessionState (internal)


Manages:

- Analyzer (for column/data type resolution)

- Optimizer (Catalyst logical plan rewriting)

- Planner (physical plan generation)

- SQL parser

- Session-local function registry

- Session-local temp views

This is where DataFrame transformations are interpreted and compiled into DAGs.


### SharedState (internal)

Global objects shared across sessions:

- Hive Metastore (if enabled)

- Global temp views

- Global configs

- TableManager (in Delta, Iceberg, etc.)

### spark.extensions (advanced)

Custom Catalyst rules and strategies can be injected:

- Custom SQL parsers

- Custom optimizer rules

- Example: integrating Delta Lake or Iceberg

## Diff between spark session and spark context.

A spark session can have

```scala
import org.apache.spark.sql.SparkSession

// The getOrCreate means the SparkSessionBuilder will try to get a spark session if there is one already created or create a new one
// and assigns the newly created SparkSession as the global default

// Note that enableHiveSupport here is similar to creating a HiveContext and all it does is enables access to Hive 
// metastore, Hive serdes, and Hive udfs.
val spark = SparkSession.builder
.appName("SparkSessionExample") 
.master("local[4]") 
.config("spark.sql.warehouse.dir", "target/spark-warehouse")
.enableHiveSupport()
.getOrCreate
```

## Related works

Create multi session and context: https://medium.com/@achilleus/spark-session-10d0d66d1d24 
