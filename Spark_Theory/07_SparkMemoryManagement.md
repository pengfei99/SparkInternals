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
|        - Free Heap     
                - driver objects, dataset schema, etc.  |
|               - JVM Overhead                        |
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
                  - driver created objects          |
                  - dataset schema                 |
                  - metadata                        |
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
# this fix the max value of the JVM which runs the spark session
spark.driver.memory = 16g

# the percentage of the Unified Spark memory in total JVM
spark.memory.fraction = 0.7

# the percentage of the storage memory in Unified Spark memory 
spark.memory.storageFraction = 0.5
```

We will have the below memory layout:

- `Unified Spark memory`, reserved for Executor(storage + compute):	16 × 0.7 = 11.2 GB	in total, 0.7 defined by spark.memory.fraction
      - `Storage memory`, for storing DF/RDD cache, broadcast: 11.2 × 0.5 = 5.6 GB, 0.5 defined by spark.memory.storageFraction 
      - `Execution memory`, for data procssing operations such as Shuffle, join, sort:	11.2 - 5.6 = 5.6 GB	
- `Free heap`, Driver objects, JVM overhead : 16 - 11.2 ≈ 4.8 GB 

### 1.2 What part of the memory is used by Spark executor threads

Spark executor uses the memory in `Spark Unified Memory Region`

#### 1.2.1 Operations in executor threads which consume execution memory (within the unified region)

Below are the operations in `executor threads` that consume `execution memory`
- columnar batch processing
- hash join tables
- sort buffers
- shuffle serialization
- window functions
- aggregates

> When execution memory runs out → spill to disk.
> 
#### 1.2.2 Operations in executor threads which consume storage memory (within the unified region)

Below are the operations in `executor threads` that consume `storage memory`.

- .cache() / .persist() DataFrames
- broadcast variables
- cached RDD partitions
- shuffle read blocks

> If storage memory is insufficient and execution needs memory → Spark evicts cached blocks.
> 
> 
### 1.3 What part of the memory is used by Spark driver

Driver uses `Free Heap (non–unified heap)`, and in general the free heap is about(~20–40%) of the total JVM memory:

It stores:
- Catalyst plans (logical/physical)
- Dataset schemas
- Driver-created objects
- Accumulators
- Local variables in your Python code
- Results from collect(), toPandas() etc.

> When the Free Heap fills up, the driver will crash with OOM, even if unified memory has free space.
> 
> 
### 1.4 What part of the memory is used by JVM overhead.

We know that JVM needs memory to do operations or store objects such as:
- JIT
- threads
- GC
- class metadata    

All these operations uses `Free Heap (non–unified heap)`


### 1.5 Why local mode causes more OOM / write failures ?

In a `cluster`, the driver heap, executor heap, shuffle memory are separated.

In `local` mode, everything is inside one heap. This causes:
- shared memory pressure
- large metadata from the driver spills into executor memory
- executor spilling slows down parquet writing
- large shuffle → GC thrashing → timeouts
- collecting or broadcasting large objects → immediate OOM