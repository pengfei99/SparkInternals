import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object WordCount {
  // Step1 : create spark session
  val spark=SparkSession.builder().master("local[4]").appName("WordCount").getOrCreate()

  def main(args: Array[String]): Unit ={
    // step2: create data
    val data=Seq("hadoop spark","hadoop flume","spark kafka")
    val textRdd=spark.sparkContext.parallelize(data)

    // step3: process data
    // split data by using " "
    // long version
    // val splitRdd=textRdd.flatMap(item=>item.split())
    // short version, when we only have one arg in the lamda expression. we can replace by it by _
    // in another word item=>item is replaced by _ (this is a scala shortcut)
    val splitRdd=textRdd.flatMap(_.split())

    // assign a init counter to each word,
    // long version
    // val tupleRdd=splitRdd.map(item=>(item,1))
    // short version
    // similar to above shortcut, we can replace item=> (item,1) by (_,1)
    val tupleRdd=splitRdd.map((_,1))

    // aggregate the word count
    // long version, when we apply reduceByKey on a Rdd, it will group all Rdd that have the same key, the accumulate
    // their value one by one, here current is the value of current rdd, accumulate is the accumulator of all values that
    // have been aggregated.
    // we can also shortcut this by using _+_
    // val reduceRdd=tupleRdd.reduceByKey((current,accumulator)=>current+accumulator)
    // here the first _ presents current, the second presents accumulator
    val reduceRdd=tupleRdd.reduceByKey(_+_)

    // step4: get result, here we need to convert the result rdd to string
    // the result rdd is like an array of (key, value), key is word, value is the wordcount
    // here we can't use short cut, because we don't have _ in the input as item.
    val strRdd=reduceRdd.map(item=>s"${item._1}, ${item._2}")

    // collect rdd to driver
    strRdd.collect().foreach(item=>println(item))

    // step5: close spark session
    spark.sparkContext.stop()
  }
}