package com.cqz.spark.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object ForeachPartitionTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("TestMapAndMapPartitions")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6))
    /**
     * mapPartitions是一个转化操作
     * mapPartitions每次处理一批数据
     * 将 rdd分成x批数据进行处理
     * p是其中一批数据
     * mapPartitions返回一批数据（iterator）
     * mapPartitions返回值必须是迭代器(iterator)
     */
     val v_value: RDD[Int] = rdd.mapPartitions(p => {
      var arr: ArrayBuffer[Int] = new ArrayBuffer[Int]()
      p.foreach(ele => {
        arr.+=(ele)
      })
      arr.foreach(println) //没有触发action,打印不出来
      arr.iterator
    })
    //v_value.foreach(println)  //触发action，可以打印出来

    println("================================")

    /**
     * foreachPartition是一个action操作
     */
     val f_value: Unit = rdd.foreachPartition(p => {
      var arr: ArrayBuffer[Int] = new ArrayBuffer[Int]()
      p.foreach(ele => {
        arr.+=(ele)
      })

      arr.foreach(println) //可以打印
    })

  }
}
