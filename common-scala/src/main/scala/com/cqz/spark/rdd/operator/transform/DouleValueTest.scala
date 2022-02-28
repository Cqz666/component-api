package com.cqz.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object DouleValueTest {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3,4,5,6))

    //交集
    val value1: RDD[Int] = rdd1.intersection(rdd2)
    println(value1.collect().mkString(","))
    //并集
    val value2: RDD[Int] = rdd1.union(rdd2)
    println(value2.collect().mkString(","))
    //差集
    val value3: RDD[Int] = rdd1.subtract(rdd2)
    println(value3.collect().mkString(","))
    //拉链
    val value4: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(value4.collect().mkString(","))

    sc.stop()
  }
}
