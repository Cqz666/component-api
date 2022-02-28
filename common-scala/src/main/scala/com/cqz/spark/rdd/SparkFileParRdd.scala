package com.cqz.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkFileParRdd {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)

    val rdd: RDD[String] = sc.textFile("data/3.txt",2)

    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
