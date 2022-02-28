package com.cqz.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkFileRdd {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)

    val seq: Seq[Int] = Seq(1,2,3,4)
    val rdd: RDD[String] = sc.textFile("data/*")
    rdd.collect().foreach(println)

    sc.stop()
  }
}
