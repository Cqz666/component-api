package com.cqz.test

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Hello world!
 *
 */
object App {
  val path ="D:\\Project\\component-api\\common-scala\\src\\main\\resources\\a"
  def main(args: Array[String]): Unit = {
    println( "Hello World!" )
    lazy val words = scala.io.Source.fromFile(path).mkString
    println(words)

    //List
    new Array[Int](10)
    val b = new ArrayBuffer[Int]()
    b+=(1,2,3,4,5)
    for(i<-b if i %2==0){
      println(i)
    }
    val sum = Array(1,2,3,4,5).sum
    println(sum)

    val c = ArrayBuffer(1,6,2,9)
    val sorted = c.sorted
    println(sorted)

    //map
    val map = Map("alice"->10,"bob"->20,"mike"->30)
    val scores = mutable.HashMap("alice"->10,"bob"->20,"mike"->30)
    val i = map("alice")
    val x = map.getOrElse("alice1",0)
    println(map)
    println(i)
    println(x)
    scores += ("sx"->1)
    println(scores)
    val set = scores.keySet
    println(set)
    for (x<-scores.values)println(x)

    //Tuple
    val t = (1,3.14,"pai")
    val second = t._2
    println(second)

    (1 to 10).map("*" * _).foreach(println)





  }
}
