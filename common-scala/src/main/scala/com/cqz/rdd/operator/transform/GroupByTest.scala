package com.cqz.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupByTest {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)

    val value: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ))
    //(a,CompactBuffer(1, 2, 3))
    //(b,CompactBuffer(4))
     val out: RDD[(String, Iterable[Int])] = value.groupByKey()
    out.collect().foreach(println)
    //(a,CompactBuffer((a,1), (a,2), (a,3)))
    //(b,CompactBuffer((b,4)))
    val out2: RDD[(String, Iterable[(String, Int)])] = value.groupBy(_._1)
    out2.collect().foreach(println)

    sc.stop()
  }
}
