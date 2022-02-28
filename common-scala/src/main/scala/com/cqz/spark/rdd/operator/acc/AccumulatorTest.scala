package com.cqz.spark.rdd.operator.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)
    val in: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val acc: LongAccumulator = sc.longAccumulator("sum")
    in.foreach(num=>acc.add(num))
    println(acc.value)

    sc.stop()

  }
}
