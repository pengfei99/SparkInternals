# 3. Spark RDD physical plan of a Job

In previous section, we have seen the logical plan of rdd execution. Note, logical plan only exists in the spark driver, it describes
how the RDD are transformered step by step.


The physical plan describes how the tasks are executed on the cluster. In this section, we will see 
1. how to generate a physical plan(e.g. Jobs, stages and tasks) based on the logical plan.
2. how the plan is executed.

- To avoid data exchange between executors(nodes), spark group all task of a partition that does not require shuffle 
  on the same executor. 
- One executor is a JVM process. It has a thread pool to run multiple task at same time. So each task is a thread.

## 3.1 Jobs, Stages and tasks
In the data pipeline, when we encounter an **action**, a **job** will be created. A spark job will be divided into stages
based on **shuffle dependencies**. Inside each stage, each partition of the data will be processed by a task. 

To understand how spark determines these stages and tasks, we can use the following strategy.
1. Check **action** to determine each job.
2. Inside each job, create a final stage, then check backwards from the final RDD (the rdd that calls the action), add 
each NarrowDependency **transformation** into the current stage. 
3. continue step 2 until you encounter a **ShuffleDependency (wide dependency)**. When we reach a shuffle, we
need to create a new stage. Repeat step 2 in the newly created stage.
4. After we determine all the stages in spark jobs, in each stage, the task number is determined by the partition 
number of the last RDD in the stage. Note, we have shuffle operations that can change the partition number of the result RDD
(e.g. repartition, coalesce, groupBy, reduceBy, etc.)

### 3.1.1 An example 
Let's consider the following example. You can find the full code example in
- scala version: src/main/java/org/pengfei/example
- python version: notebooks/word_count.ipynb

```scala
    val  textRdd:RDD[String] = spark.sparkContext.textFile("sourceFile.txt")

    val splitRdd:RDD[String]=textRdd.flatMap(_.split(" "))

    val tupleRdd:RDD[(String,Int)]=splitRdd.map((_,1))

    val reduceRdd:RDD[(String,Int)]=tupleRdd.reduceByKey(_+_)

    val strRdd:RDD[String]=reduceRdd.map(item=>s"${item._1}, ${item._2}")

    // collect rdd to the driver
    strRdd.collect().foreach(item=>println(item))
```

Let's follow the procedure of 3.1 to determine job, stage, tasks.

1. There is only one action **collect()**, so there is only one job in this spark application.
2. We create a final stage **stage 0**, and put strRDD (final RDD) in it.
   - strRDD is created by reduceRdd.map(), it's a narrow dependency. add reduceRdd into the same stage
   - reduceRdd is created by tupleRdd.reduceByKey(), it's a shuffle dependency, create a new stage.
3. Create a shuffleMap stage **stage 1**, put tupleRdd in stage 1.
   - tupleRdd is created by splitRdd.map(), it's a narrow, add splitRdd into stage 1.
   - splitRdd is created by textRdd.flatMap(), it's a narrow, add textRdd into stage 1.
   - textRdd is created by spark.sparkContext.textFile("sourceFile.txt"). which is the source RDD. stop process

You can find the result physical plan in the following figure.

![spark_rdd_physical_plan](https://raw.githubusercontent.com/pengfei99/SparkInternals/main/img/spark_rdd_physical_plan.PNG)

If you run the above code, you can find the generated dag in the spark ui in the following figure.

![spark_ui_physical_plan](https://raw.githubusercontent.com/pengfei99/SparkInternals/main/img/spark_ui_physical_plan.PNG)

Note, if you are using the pyspark version of the word count, you may have a different physical plan in the spark ui.
That's because py4j translate and optimize your python code to java differently.
Below is an example of the pyspark physical plan
![pyspark_ui_physical_plan](https://raw.githubusercontent.com/pengfei99/SparkInternals/main/img/pyspark_word_count_physical_plan.png)

