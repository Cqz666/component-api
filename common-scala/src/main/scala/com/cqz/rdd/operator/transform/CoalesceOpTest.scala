package com.cqz.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CoalesceOpTest {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)

    val in: RDD[Int] = sc.makeRDD(List(1,2,3,4),4)
    //缩减分区 默认不shuffle, 但数据可能不均， true为shuffle
    //扩大分区可以用repartition 底层是coalesce进行shuffle
    val out: RDD[Int] = in.coalesce(2)
//    val out: RDD[Int] = in.coalesce(2,true)
    out.saveAsTextFile("output")


    sc.stop()
  }
}
