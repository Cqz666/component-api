package com.cqz.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SortByOpTest {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)

    val in: RDD[(String, Int)] = sc.makeRDD(List(("2",1),("1",5),("3",3)))
    in.sortBy(_._1.toInt).collect().foreach(println)

    sc.stop()
  }
}
