package com.cqz.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkMemoryParRdd {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
//    sparkconf.set("spark.default.parallelism","2")
    val sc = new SparkContext(sparkconf)


    //    val rdd = sc.parallelize(seq)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
//    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
