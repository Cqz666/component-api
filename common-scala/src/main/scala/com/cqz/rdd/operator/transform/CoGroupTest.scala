package com.cqz.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CoGroupTest {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)

    val in1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2)))
    val in2: RDD[(String, Int)] = sc.makeRDD(List(("a",4),("b",5),("c",6),("c",7)))
    //连接 分组
    val out: RDD[(String, (Iterable[Int], Iterable[Int]))] = in1.cogroup(in2)
    out.collect().foreach(println)

    sc.stop()
  }
}
