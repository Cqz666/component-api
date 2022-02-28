package com.cqz.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 随机抽样算子
  */
object SampleOpTest {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)

    val in: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))
    println(in.sample(
      false, // 不返回数据，数据不重复
      0.4, //每条数据被抽中的概率
      3 //随机算法种子
    ).collect().mkString(","))

    sc.stop()

  }

}
