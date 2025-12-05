# Spark Memory Management

As spark can run in different mode(e.g. local, k8s, yarn). The memory management is quite different.

## 1. Local mode Spark Memory Management

**In local mode, spark driver and spark worker runs in the same JVM process**.
As a result, they both use the same JVM heap:
The `Driver` uses the heap for:
- Query planning
- Dataset metadata
- Collect result storage
- Broadcast management
- Task scheduling

The `Executor (Worker)` uses the same heap for:
- Columnar batches
- Shuffle blocks
- Joins, aggregations
- Parquet write buffers
- Cached blocks

Below figure is the geneal JVM memory layout of a spark session

```text
+----------------------------------------------------+
|                    JVM HEAP                        |
|        - Spark Unified Memory Region                |
|        - Free Heap                                  |
|        - JVM Overhead                               |
+----------------------------------------------------+
|            Spark Unified Memory Region             |
|           (execution + storage memory)             |
|          spark.memory.fraction:                    |
|       - best value range 0.6–0.8,default value 0.6 |
|----------------------------------------------------|
|      Storage Memory (cached DF/RDD blocks)         |
|             spark.memory.storageFraction:          |
|            - best value range: 0.3~0.5             |
|             - default value: 0.5                    |
|----------------------------------------------------|
|      Execution Memory (runtime compute)            |
|      shuffle, join buffers, sorts, aggregates      |
|      Kryo Buffers                                  |
|----------------------------------------------------|
|                     Free heap                      |
|               Spark related:                      |
                  - metadata, Driver code,          |
|                 - Executor management, JVM objects |
|               JVM Overhead (Not spark related)      |
|                 - JIT,                              |
|                  - threads,                       |
|                  - GC,                            |
|                  - class metadata                  |
+----------------------------------------------------+

```


In Spark, `execution` and `storage` memory share a `Unified Memory Region (M)`.

- `Execution memory`: are used for computation in shuffles, joins, sorts and aggregations. If `Kryo serialization` is used. `Kryo Buffers` is in execution memory too. 
- `Storage memory` : are used for caching and propagating internal data across the cluster. 

### 1.1 Memory allocation example

With the below spark session configuration

```shell
spark.driver.memory = 16g
spark.memory.fraction = 0.7
spark.memory.storageFraction = 0.5
```

We will have the below memory layout

Region	Size	Purpose
Unified Spark memory	16 × 0.7 = 11.2 GB	Executor compute + cached blocks
Storage memory	11.2 × 0.5 = 5.6 GB	DF/RDD cache, broadcast
Execution memory	11.2 - 5.6 = 5.6 GB	Shuffle, join, sort
Free heap	≈ 4.8 GB	Driver + metadata objects