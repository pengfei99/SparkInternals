# SparkInternals

In this project, I will try to show you the internal mechanism of a spark application. I will use RDD to illustrate. Because the DataFrame and DataSet are built upon RDDs, but they will use optimizer such as **[Catalyst and Tungsten](https://www.linkedin.com/pulse/catalyst-tungsten-apache-sparks-speeding-engine-deepak-rajak/)**. So it's easier to understand the inner working of a spark application.

The plan is:
1. Introduction of the key term and concept
2. Understand the spark cluster architecture
3. Understand how a spark application starts, runs and finishes
4. Use a spark application to illustrate how spark generate a logic plan, physical plan to execute a job via RDD (a job is trigger by a spark action).
5. How the plan is executedd
6. How the shuffle works in spark
