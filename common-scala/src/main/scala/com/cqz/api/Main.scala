package com.cqz.api

import scala.collection.mutable.ArrayBuffer

object Main {
  def main(args: Array[String]): Unit = {
    val str = "vid,game_id".split(",").map(e => e.concat(s" <> '' and $e is not null ")).mkString(",")
    println(str)

    val errRes = new ArrayBuffer[String]()
    errRes.append("a")
    errRes.append("b")
    errRes.append("c")
    println(errRes.toIterator.length)


  }
}
