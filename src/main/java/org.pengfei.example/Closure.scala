package org.pengfei.example

import org.pengfei.example.Closure.closure

/* In this class, we demonstrate a closure function. A closure is a record storing a function[a] together with an
* environment. The environment is a mapping associating each free variable of the function (variables that are used
* locally, but defined in an enclosing scope) with the value or reference to which the name was bound when the
* closure was created. Unlike a plain function, a closure allows the function to access those captured variables
* through the closure's copies of their values or references, even when the function is invoked outside their scope.
*
*/
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
