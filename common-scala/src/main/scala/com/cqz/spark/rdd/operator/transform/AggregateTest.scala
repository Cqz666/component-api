package com.cqz.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AggregateTest {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)

    val in: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("a", 4)
    ),2)
    //参数列表1：初始值0
    //参数列表2：1 分区内计算规则 2 分区间计算规则
    val out: RDD[(String, Int)] = in.aggregateByKey(0)((x,y)=>math.max(x,y),(x,y)=>x+y)

    out.collect().foreach(println)
  }
}
