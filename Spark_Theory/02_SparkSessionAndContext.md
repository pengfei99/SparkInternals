# Deep dive to spark session and context
Create multi session and context
https://medium.com/@achilleus/spark-session-10d0d66d1d24

# Diff between spark session and spark context.
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
 
