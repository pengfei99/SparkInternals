# 6. Spark closure function

In spark, we have a lot of high dimensional functions which take function as argument. The input function may come with
its own scope. When spark distributes these functions to each executor, spark needs to serialize these functions. If 
the enclosed environment(scope) is not serialized as the function, the function will not run correctly.

## 6.1 What is Closure function?

A closure is a record storing a function together with an environment(scope). The environment is a mapping 
associating each free variable of the function (variables that are used locally, but defined in an enclosing scope) 
with the value or reference to which the name was bound when the closure was created. Unlike a plain function, 
a closure allows the function to access those captured variables through the closure's copies of their values or 
references, even when the function is invoked outside their scope.

Below example shows a simple closure function. Inside closure, we have a variable factor defined inside the scope of 
closure.

```scala
object Closure {

  def main(arg:Array[String]):Unit={
    // here f is a closure function, In scala function has a unique type "FunctionX", where X is the number of argument
    val f:Int=>Double =closure()
    val res=f(5)
    print(s"the circle area of rayon 5 is: ${res}")
  }
  /*
  * Inside the closure function, we have a variable factor. This variable can't be accessed inside the main
  * function directly. Because it only exists inside the "scope" of closure function.
  * If we call the closure() inside the main, then main can access(Indirectly) the factor value to calculate the area.
  * */
  def closure():Int => Double={
    val factor=3.13
    val areaFunction= (r:Int) => math.pow(r,2)*factor
    areaFunction
  }

}
```

A closure function can enclose a scope inside its function, it can enclose a scope outside. Check the following example,
The method doStuff **encloses an outside variable**. As a result, the doStuff closure function enclose the **scope of MyClass**. 

```scala
import org.apache.spark.rdd.RDD
Class MyClass{
  val field="hello"
  
  def doStuff(rdd:RDD[String]):RDD[String]={
    rdd.map(x=>field+x)
  }
}
```

## 6.2 The problem of closure function inside spark.

Image we call the doStuff() function inside my spark application. To run map, the spark driver needs to serialize doStuff
and send it to the executors. As doStuff is a closure function, the spark driver needs to serialize the enclosed scope.
In our case, the enclosed scope is the **MyClass**. Check the below figure.

![spark_closure_function](https://raw.githubusercontent.com/pengfei99/SparkInternals/main/img/spark_closure_function.PNG)

In a normal case, this would not be a big problem. We may lose some performance, because serialization of the class 
from the spark driver and deserialization from the executor both take time. But imagine two scenarios:
1. If **MyClass** is not serializable, this will cause your spark application to fail.
2. If **MyClass** contains large list/array, and you have many tasks(one task for one partitions), spark will make many 
copies (one copy for one task) of **MyClass** and send them to each task. 


## 6.3 Conclusion

Avoid using closure function in spark as much as possible. If you have no choice, make sure the enclosed scope is the
smallest possible.

