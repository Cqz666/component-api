package com.cqz.test

import scala.collection.mutable.ListBuffer

object CollectionTest {
  def main(args: Array[String]): Unit = {
    val event = "play_app,play_game"
    val eventList = new ListBuffer[String]()
    for (elem <- event.split(",")) {eventList+="'"+elem+"'"}
    eventList.insert(0,"a")
    eventList.insert(1,"b")

    val eventStr: String = eventList.mkString(",")
    println(eventStr)
  }
}
