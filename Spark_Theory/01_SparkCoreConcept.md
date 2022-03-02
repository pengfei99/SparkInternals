# 1. Spark core concept

Before diving into the details of Spark internals, it is important to have a high-level understanding of the 
core concepts and the various core components in Spark. 

Infrastructure part:
- Spark Cluster:
- Spark Cluster Manager: Local, Standalone, YARN, Mesos, K8s
- Spark Master node: (The node who can accept your spark submit request or spark-shell connection)
- Spark worker node: A worker offers resources (memory, CPU, etc.) to the cluster manager, it may contain one or more 
               executors which perform the tasks that assigned by the spark driver.


## 1.1 Concept from Infrastructure

### 1.1.1 Spark Cluster and cluster manager:

A Spark cluster is a collection of servers that can run spark jobs.

To efficiently and intelligently manage a collection of servers, the Spark cluster uses a **resource manager**.
For now spark support the following resource managers:
- Local (no cluster)
- Standalone (Spark native resource manager)
- Apache YARN 
- Apache Mesos
- K8s.


The cluster manager knows where the workers are located, how much memory they have, and the number of CPU cores each 
one has. One of the main responsibilities of the cluster manager is to orchestrate the work by assigning it to each worker.

### 1.1.2 Master node 
The master node is responsible for handling spark submit request or spark shell connection. Depends on different resource
manager that the cluster used, the master node negotiates with the resource manager to get required resources. 
The spark driver's location may not be on the master node.

#### 1.1.2.1 Standalone, 
The official [doc](http://spark.apache.org/docs/latest/spark-standalone.html) of the standalone mode. 

When you submit a job with the following command, the standalone cluster allows **client mode and cluster mode**. 
```shell
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]
```

- In client mode, the driver is launched in the same process as the client that submits the application. 
- In cluster mode, however, the driver is launched from one of the Worker processes inside the cluster, and the client 
process exits as soon as it fulfills its responsibility of submitting the application without waiting for the application 
to finish.

When you run an interactive Spark shell against the cluster with the following command, spark is always in client mode.
```shell
./bin/spark-shell --master spark://IP:PORT
```
##### Standalone Architecture Overview
Below figure shows an overview of a spark cluster with standalone resource manager and a submit in cluster mode, it has:
- one master node
- three worker node

In the master node, there is a **master daemon** which can handle spark submit request. This daemon also handles the 
communication with workers.

In the worker node, there is a **worker daemon** :
- during resource scheduling time, worker daemon creates executor by creating and using another process called ExecutorBackend
  Note one executorBackend only handles one executor(creation, termination).
- during job execution time, it receives tasks from driver, run the tasks on executor and send result back to driver.
  note the driver also runs on a worker in cluster mode.
- After the job done, worker daemon close the **Executor** via **ExecutorBackend**

When there is an **action** in the rdd, df or ds process, the worker will send result to the driver, in cluster mode, 
this happens inside the cluster.

![spark_cluster_architecture_overview](https://raw.githubusercontent.com/pengfei99/SparkInternals/main/img/spark_running_application_architecture_overview.PNG)


#### 1.1.2.2 yarn
The official [doc](http://spark.apache.org/docs/latest/running-on-yarn.html) of the yarn mode.
The driver's location, it's similar to standalone mode.

#### 1.1.2.3 k8s
The official [doc](https://spark.apache.org/docs/latest/running-on-kubernetes.html) of the k8s mode.

spark-submit can be directly used to submit a Spark application to a Kubernetes cluster. The submission mechanism 
works as follows:

- Spark creates a Spark driver running within a Kubernetes pod(**driver never runs on the submitter, so no client mode**).
- The driver creates executors which are also running within Kubernetes pods and connects to them, and executes application code.
- When the application completes, the executor pods terminate and are cleaned up, but the driver pod persists logs and 
  remains in “completed” state in the Kubernetes API until it’s eventually garbage collected or manually cleaned up.

**The driver and executor pod scheduling is handled by Kubernetes. Communication to the Kubernetes API is done via fabric8.** 

![](https://raw.githubusercontent.com/pengfei99/SparkInternals/main/img/spark_on_k8s.PNG)
### 1.1.3 Worker node

A worker offers resources (memory, CPU, etc.) to the cluster manager and performs the assigned work.





## 1.2 Concept from Software part:

### 1.2.1 Spark application and drivers

#### Spark application
A **spark application** is a self-contained computation that runs user-supplied code to compute a result. 
It consists of a driver program (the driver can be on client side or cluster side) and executors on the cluster.
A spark application can have multiple jobs.

#### Spark driver
**The driver is the process where the main method runs**. The driver will not only reserve resource and create executor, 
but also schedule the task on the executors.


##### Driver reserve resource
First the driver will call **ClusterManager** to reserve resource and create executors on the worker nodes based on 
the spark context/conf definition. Note for different spark cluster mode(e.g. stand alone, k8s, yarn, etc.), the ClusterManager
can be different.

After the creation of executor, driver and executor communicate directly. 

##### Driver 
The driver/SparkContext converts Spark Application (the user program main method) ->  Driver -> Jobs->Stages->Tasks 
and after that it schedules the tasks on the executors.


                     
### 1.2.2 job

A job is a parallel computation consisting of multiple stages. Jobs are divided into "stages" based on the **shuffle
boundary**. Data shuffling means data are transferred from one executor to another executor (These executors can 
be on the same node or different node).

Operations such as **partitionBy, groupBy, orderBy** will lead to data shuffling. Thus trigger a new stage.

### 1.2.3 stage: 

A stage groups a list of operations that can be calculated on the same executor (no shuffle needed). 

The possible operation name:
- WholeStageCodegen:
- mapPartitions

### 1.2.4 task: 

### 1.2.5 SparkSession: 
SparkSession was introduced in version 2.0 and is a unified entry point to access underlying API such as RDD, 
DataFrame and DataSet. 

In spark-shell, the object spark (SparkSession) is available by default. In an interactive context, it can be 
created programmatically using SparkSession builder pattern(e.g. SparkSession.builder.master.config(...).getOrCreate).

### 1.2.6 SparkContext
Prior to Spark 2.0, Spark Context was the entry point of any spark application. It uses a sparkConf 
which had all the cluster configs and parameters to create a Spark Context object. But it can just use RDDs API, 
To use spark SQL, we need to create SQLContext, To use hive, we need HiveContext. To use streaming, we need Streaming Application.

### 1.2.7 Spark executor

Executors are worker nodes' jvm processes in charge of running individual tasks in a given Spark job. 

- **In cluster mode, each executor is a separate jvm instances.** 
- In local mode, spark starts only a single JVM to emulate all components (i.e. driver and executors)

Executors are launched at the beginning of a Spark application and typically run for the entire lifetime of 
an application. Once they have run the task they send the results to the driver. 

They also provide in-memory storage for RDDs that are cached by user programs through **Block Manager**. 

When executors are started they register themselves with the driver and from so on they communicate directly. 
The workers are in charge of communicating the cluster manager the availability of their resources.
A worker can have multiple executor 


# Spark Data structures

- RDD
- Dataframe
- Dataset

##  RDD
RDD has five main features:
- partition data
- compute logic
- rdd dependencies

### RDD Dependency types and the optimization at DAGScheduler
– **Narrow dependency**:  each partition of the parent RDD is used by at most one partition of the child RDD. 
           This means the task can be executed locally, and we don’t have to shuffle. (Eg: map, flatMap, Filter, sample)
– **Wide dependency**: multiple child partitions may depend on one partition of the parent RDD. 
           This means we have to shuffle data unless the parents are hash-partitioned (Eg: sortByKey, reduceByKey, groupByKey, cogroupByKey, join, cartesian)

Thanks to the lazy evaluation technique, the **DAGScheduler will be able to optimize the stages before submitting the job**:
- pipelines narrow operations within a stage,
- picks join algorithms based on partitioning (try to minimize shuffles), 
- reuses previously cached data.

# Spark application execution flow

A spark application may contain multiple jobs. When an action is called on an RDD (dataframe and dataset is the abstraction
                     of RDD), the SparkContext will submit a job to the DAGScheduler. 


Starting by creating a Rdd object by using SparkContext, then we transform it with the filter transformation and finally call action count. When an action is called on rdd, the SparkContext will submit a job to the DAGScheduler – where the very first optimizations happen.

The DAGSchedule receives target Rdds, functions to run on each partition (pipe the transformations, action), and a listener for results. It will:
– build Stages of Task objects (code + preferred location)
– submit them to TaskScheduler as ready
– Resubmit failed Stages if outputs are lost

The TaskScheduler is responsible for launching tasks at executors in our cluster, re-launch failed tasks several times, return the result to DAGScheduler.

We can now quickly summarize:
- We submit a jar application which contains jobs
- The job gets submitted to DAGScheduler via SparkContext will be split in to Stages. The DAGScheduler schedules the run order of these stages.
- A Stage contains a set of tasks to run on Executors. The TaskScheduler schedules the run of tasks.

1. When you submit a spark application to the cluster in cluster mode, the cluster will find a worker to create
   a driver of your spark application that runs the main() of your spark application. Inside the driver, we have a
   spark context and the data pipeline of how you transform your data. The driver analyse the data pipeline and generate
   logical plans of your spark applications. Note a spark application has multiple job, a job has multiple stages, a stage
   has multiple tasks.

2. If the driver encounter a **rdd.action()**, it calls **DAGScheduler.runJob(rdd, processPartition, resultHandler)
   to create a job**. If no action, it just adds more stages into the job.

3. DAGScheduler's runJob() calls **submitJob(rdd, func, partitions, allowLocal, resultHandler) to submit a job**.
4. submitJob() gets a **jobId**, then wrap the function once again and send a **JobSubmitted message** to **DAGSchedulerEventProcessActor**.
   Upon receiving this message, the actor calls **dagScheduler.handleJobSubmitted() to handle the submitted job**.
   This is an example of **event-driven programming model**.
5. **handleJobSubmitted() firstly calls finalStage = newStage()** to create stages, then it submitStage(finalStage).
   If finalStage has parents, the parent stages will be submitted first. In this case, finalStage is actually submitted
   by submitWaitingStages().
6. Each stage has a corresponding taskSet which contains computation tasks for each partition, DAGScheduler sends the stages(taskSets) to
   the **TaskScheduler** and ask it to run tasks on the executors.

Below figure shows a graphical representation of the above process

![spark_application_submission_process](https://raw.githubusercontent.com/pengfei99/SparkInternals/main/img/spark_application_submission_process.PNG)


Transformation

| Operation Name | Description |
|-------------------------| ------------------------------------------|
| map(f: T => U) | Return a MappedRDD[U] by applying function f to each element |
| flatmap(f: T => TraversableOnce[U]) | Return a new FlatMappedRDD[U] by first applying a function to all elements and then flattening the results. |
| filter(f: T => Boolean) | Return a FilteredRDD[T] having elemnts that f return true |
|mapPartitions(Iterator[T] => Iterator[U])	| Return a new MapPartitionsRDD[U] by applying a function to each partition |
|sample(withReplacement, fraction, seed) | Return a new PartitionwiseSampledRDD[T] which is a sampled subset |
|union(otherRdd[T])	| Return a new UnionRDD[T] by making union with another Rdd |
|intersection(otherRdd[T]) | Return a new RDD[T] by making intersection with another Rdd|
|distinct()	| Return a new RDD[T] containing distinct elements |
|groupByKey()  | Being called on (K,V) Rdd, return a new RDD[([K], Iterable[V])] |
|reduceByKey(f: (V, V) => V) | Being called on (K, V) Rdd, return a new RDD[(K, V)] by aggregating values using feg: reduceByKey(_+_)|
|sortByKey([ascending])	| Being called on (K,V) Rdd where K implements Ordered, return a new RDD[(K, V)] sorted by K |
|join(other: RDD[(K, W))| Being called on (K,V) Rdd, return a new RDD[(K, (V, W))] by joining them |
|cogroup(other: RDD[(K, W))	| Being called on (K,V) Rdd, return a new RDD[(K, (Iterable[V], Iterable[W]))] such that for each key k in this & other, get a tuple with the list of values for that key in this as well as other|
|cartesian(other: RDD[U]) | Return a  new RDD[(T, U)] by applying product |



Actions

|Operation name | Description |
|-------------------------| ------------------------------------------|
| reduce(f: (T, T) => T) | return T by reducing the elements using specified commutative and associative binary operator |
| collect()| Return an Array[T] containing all elements|
|count()	| Return the number of elements |
|first()	| Return the first element|
|take(num)	| Return an Array[T] taking first num elements|
|takeSample(withReplacement, fraction, seed) | Return an Array[T] which is a sampled subset|
|takeOrdered(num)(order) | Return an Array[T] having num smallest or biggest (depend on order) elements|
|saveAsTextFile(fileName) | |
|saveAsSequenceFile(fileName) | |
|saveAsObjectFile(fileName)	| Save (serialized) Rdd |
|countByValue()	| Return a Map[T, Long] having the count of each unique value |
|countByKey() |	Return a Map[K, Long] counting the number of elements for each key |
|foreach(f: T=>Unit) |	Apply function f to each element |