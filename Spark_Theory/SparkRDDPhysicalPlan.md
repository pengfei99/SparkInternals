# 3. Spark RDD physical plan of a Job

In previous section, we have seen the logical plan of rdd execution. Note, logical plan only exists in the spark driver, it describes
how the RDD are transformered step by step.


The physical plan describes how the tasks are executed on the cluster. In this section, we will see 
1. how to generate a physical plan based on the logical plan.
2. how the plan is executed.

- To avoid data exchange between executors(nodes), spark group all task of a partition that does not require shuffle 
  on the same executor. 
- One executor is a JVM process. It has a thread pool to run multiple task at same time. So each task is a thread.

## 3.1 Stages and tasks
We know that a spark job will be divided into stages, then each stage is divided into tasks. To understand how spark 
determines these stages and tasks, we can use the following strategy.
- check backwards from the final RDD, add each NarrowDependency into the current stage of the final RDD, 
- continue this process until you encounter a **ShuffleDependency (wide dependency)**. When we reach a shuffle, we
need to create a new stage. 
- After we determine all the stages in a spark job, in each stage, the task number is determined by the partition 
number of the last RDD in the stage. Note, we have shuffle operations that can change the partition number of the result RDD
(e.g. repartition, coalesce, groupBy, reduceBy, etc.)

Let's reuse the 