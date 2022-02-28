package com.cqz.spark.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AggregateTest {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    //aggregateByKey：只会参与分区内计算
    //aggregate：会参与分区内和分区间计算
    val result: Int = rdd.aggregate(10)(_+_,_+_)
    val result2: Int = rdd.fold(10)(_+_)
    println(result)
    println(result2)

    sc.stop()
  }
}
