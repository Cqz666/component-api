package com.cqz.spark.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CountByTest {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)),2)
    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    println(intToLong)
    val stringToLong: collection.Map[String, Long] = rdd2.countByKey()
    println(stringToLong)

    sc.stop()
  }
}
