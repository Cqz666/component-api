package com.cqz.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapOperator {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)

    val in: RDD[List[Int]] = sc.makeRDD(List(List(1,2),List(3,4)))
     val in2: RDD[Any] = sc.makeRDD(List(List(1,2),3,List(4,5)))

    val out: RDD[Int] = in.flatMap(list=>list)

    val out2: RDD[Any] = in2.flatMap {
      case list: List[_] => list
      case dat => List(dat)
    }
    out2.collect.foreach(println)

//    out.collect().foreach(println)
    sc.stop()
  }
}
