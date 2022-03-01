# Spark web UI

Apache Spark provides a suite of web user interfaces (UIs) that you can use to monitor the 
**status and resource consumption of your Spark cluster**.

It has the following tabs:
- Jobs:
- Stages:
- Storage:
- Environment:
- Executors
- SQL:
- Structured Streaming
- Streaming(DStreams)
- JDBC/ODBC


## Jobs tab

The Jobs tab displays **a summary page of all jobs in the Spark application** and a detailed page for each job. 
The summary page shows high-level information, such as the status, duration, and progress of all jobs and the 
overall event timeline. When you click on a job on the summary page, you see the detailed page for that job. The 
detailed page further shows the event timeline, DAG visualization, and all stages of the job.

Jobs are sorted by their status (e.g. running, succeeded, failed), when you click on **description** of the job, you 
will get details of this job, and it contains:

- status of the job: ( running, succeeded, failed)
- Number of stages per status (active, pending, completed, skipped, failed)
- Associated SQL Query: Link to the sql tab for this job
- Event timeline: Displays in chronological order the events related to the executors (added, removed) and the 
                 stages of the job
- DAG visualization: Visual representation of the directed acyclic graph of this job where 
        **vertices represent the RDDs or DataFrames** and the **edges represent an operation to be applied on RDD**.
- 
