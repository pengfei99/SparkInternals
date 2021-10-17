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
  ...
  ...
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