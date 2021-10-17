# 2 Spark RDD logical plan

When writing your spark code to manipulate data, you should have a **dependency diagram(data lineage)** in your mind. 
This diagram describes the dependencies of each RDD, and how they have been transformed. A logical plan terminates when
it encounters an Action. So A logical plan is the plan of one single spark job. 

In general, a transformation takes an input RDD and outputs a new RDD as result. However, some transformations are more 
complicated and contain several sub-transformation(), which will produce more than one RDD. That's why the number of RDDs is, in fact, more than we thought

In order to make this more clear, we will talk about :

1. How to produce RDD ? 
2. What kind of RDD should be produced ?
3. How to build dependency relationship between RDDs ?

## 2.1 A simple example
Below is a simple spark application which does the word count.

```scala
// step2: create data
val data=Seq("hadoop spark","hadoop flume","spark kafka")
val textRdd=spark.sparkContext.parallelize(data)

// step3: process data
val splitRdd=textRdd.flatMap(_.split(" "))

val tupleRdd=splitRdd.map((_,1))

val reduceRdd=tupleRdd.reduceByKey(_+_)

// step4: get result, here we need to convert the result rdd to string
val strRdd=reduceRdd.map(item=>s"${item._1}, ${item._2}")

// collect rdd to the driver
strRdd.collect().foreach(item=>println(item))

// get the logical plan of the RDD transformation pipeline
print(strRdd.toDebugString)
```
By reading the above code, you can easily build a lineage rawData->textRdd->splitRdd->tupleRdd->reduceRdd->strRdd

Below figure shows a graphical representation of a **spark RDD logical plan**.

![spark_rdd_logical_plan](https://raw.githubusercontent.com/pengfei99/SparkInternals/main/img/spark_rdd_logical_plan.PNG)

In the graph, you can only see the **class type** of the RDD such as 
- ParallelCollectionRDD : textRdd is created by calling parallelize, so it has this type
- MapPartitionsRDD: splitRdd (flatMap), tupleRdd(map), strRdd(map) 
- ShuffledRDD: reduceRdd (reduceByKey)

So the first block is textRdd, after a flatMap transformation, it produces second block (splitRdd), after a Map transformation
it produces third block(tupleRdd).

The reduceByKey transformation triggers a shuffle, in spark when it encounters a shuffle, spark creates a new **stage**.
We will explain why in the **SparkPhysicalPlan chapter**. After shuffle, we have the fourth block (reduceRdd). The last block (strRdd) is
created by calling a Map.

## 2.2 How to produce RDD? 

Logical plan is essentially an RDD computing chain(lineage). In spark, every RDD has a **compute() method** which takes 
the input records of the previous RDD or data source, then performs transformation(), finally outputs computed records.

### 2.2.1 Use textFile to produce RDD

Let's retake the code example of Section 2.1. This time we read data from a file.

```scala
val fileTextRdd=spark.sparkContext.textFile("")

// Check the code source of textFile, you will find out it calls hadoopFile()
def textFile(
              path: String,
              minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
  assertNotStopped()
  hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
    minPartitions).map(pair => pair._2.toString).setName(path)
}

// And hadoopFile() actually creates a new HadoopRDD
// HadoopRDD is a subClass of RDD, and it overwrites the compute() method.
// this compute method reads data from HDFS blocks and copy data to the RDD partitions in spark
def hadoopFile[K, V](path: String,
                      inputFormatClass: Class[_ <: InputFormat[K, V]],
                      keyClass: Class[K],
                      valueClass: Class[V],
                      minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {
  
  new HadoopRDD(
    this,
    confBroadcast,
    Some(setInputPathsFunc),
    inputFormatClass,
    keyClass,
    valueClass,
    minPartitions).setName(path)
}

```

### 2.2.2 Use map and flatMap to produce RDD

Below is the spark source code of the map and flatMap function. You can notice that it creates a new MapPartitionsRDD by using
a given function f.

Note the core logic of these two functions is **iter.map(cleanF)**. You can consider iter is the collection class in
scala, then we call map(cleanF) to apply function f on the data inside the collection. When map function creates a new 
MapPartitionsRDD(previous_rdd,func), the compute() method of this new RDD will apply the func on every item of every partition
of the previous_rdd. Same for flatMap, flatMap just use **iter.flatMap(cleanF)** as func in its compute() method.


```scala
def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.map(cleanF))
  }

def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope {
  val cleanF = sc.clean(f)
  new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.flatMap(cleanF))
}
```

## 2.3 Dependency relationship between RDD

To understand the dependency relationship between RDDs, we need to figure out the following things:

1. What is the parent RDD(one or several) of an RDD **target**?
2. How many partitions are there in RDD **target** ?
3. What's the relationship between the partitions of RDD **target** and those of its parent RDD(s)?

The first question is trivial. Because when you have a transformation such as val target = rdd_a.transformation(rdd_b), 
e.g., val target = a.join(b) means that RDD **target depends both RDD a and RDD b**.

For the second question, as mentioned before, the number of partitions is defined by user, by default, it 
takes the max(numPartitions[parent RDD 1], ..., numPartitions[parent RDD n]) partition number of its parent RDD.

The third one is a little complex, we need to consider the meaning of a transformation(). Different transformation()s 
have different dependency. For example, map() is 1:1, while groupByKey() produces a ShuffledRDD in which each 
partition depends on all partitions in its parent RDD. Besides, some transformation() can be more complex.


### 2.3.1 Narrow vs Wide
In spark, there are 2 kinds of dependencies which are defined in terms of parent RDD's partition:

- NarrowDependency: When each partition at the parent RDD is used by at most one partition of the child RDD, then we 
                 have a narrow dependency. Computations of transformations with this kind of dependency are rather 
                 fast as they do not require any data shuffling over the cluster network. In addition, optimizations 
                 such as **pipelining** are also possible.
Transformation leads to narrow: map, filter and union.


- WideDependency/shuffleDependency: When each partition of the parent RDD may be depended on by multiple child partitions 
                 (wide dependency), then the computation speed might be significantly affected as we might need to 
               shuffle data around different nodes when creating new partitions.
               Wide dependency is often called as shuffleDependency. Because the spark source code has the class named
               ShuffleDependency which is a child class of Dependency.

Transformation leads to wide: groupByKey, reduceByKey and join operations whose inputs are not co-partitioned

Below figure shows the difference between narrow and wide dependency.

![spark_rdd_dependencies](https://raw.githubusercontent.com/pengfei99/SparkInternals/main/img/spark_rdd_dependencies.PNG)

### 2.3.2 Some RDD transformation dependency example

#### 2.3.2.1 Union

union() simply combines two RDDs together. It never changes the partition of its parent RDD. RangeDependency(1:1) retains 
the borders of original RDDs in order to make it easy to revisit the partitions from RDD produced by union()


![spark_union_rdd_dependencies](https://raw.githubusercontent.com/pengfei99/SparkInternals/main/img/spark_union_rdd_dependencies.PNG)


#### 2.3.2.2 GroupByKey

groupByKey() combines records with the same key by shuffle. The compute() function in ShuffledRDD fetches necessary 
data for its partitions, then take mapPartition() operation (like OneToOneDependency), MapPartitionsRDD will be 
produced by aggregate(). Finally, ArrayBuffer type in the value is cast to Iterable

groupByKey() has no map side combine, because map side combine does not reduce the amount of data shuffled and 
requires all map side data be inserted into a hash table, leading to more objects in the old gen.

ArrayBuffer is essentially a CompactBuffer which is an append-only buffer similar to ArrayBuffer, but more 
memory-efficient for small buffers.

![spark_groupByKey_rdd_dependencies](https://raw.githubusercontent.com/pengfei99/SparkInternals/main/img/spark_groupByKey_rdd_dependencies.PNG)


#### 2.3.2.3 ReduceByKey
reduceByKey() is similar to MapReduce. The data flow is equivalent. redcuceByKey enables map side combine by default, 
which is carried out by mapPartitions before shuffle and results in MapPartitionsRDD. After shuffle, 
aggregate + mapPartitions is applied to ShuffledRDD. Again, we get a MapPartitionsRDD

![spark_reduceByKey_rdd_dependencies](https://raw.githubusercontent.com/pengfei99/SparkInternals/main/img/spark_reduceByKey_rdd_dependencies.PNG)


#### 2.3.2.4 Distinct

distinct() aims to deduplicate RDD records. Since duplicated records can be found in different partitions, shuffle is 
needed to deduplicate records by using aggregate(). However, shuffle requires RDD must in [(K, V)] shape. If the 
original records as in our example have only keys, e.g. RDD[Int], then it must be transformed to <Int, null> by using
map(). After that, reduceByKey() is used to do some shuffle (mapSideCombine->reduce->MapPartitionsRDD). Finally, 
only key is taken from <K, null> by map()(MappedRDD). In the figure, the three block in blue represents the 
ReduceByKey() that we called.

![spark_distinct_rdd_dependencies](https://raw.githubusercontent.com/pengfei99/SparkInternals/main/img/spark_distinct_rdd_dependencies.PNG)
