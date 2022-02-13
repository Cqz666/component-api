package com.cqz.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PartitionByTest {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)

    val in: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val value: RDD[(Int, Int)] = in.map((_,1))

    value.partitionBy(new HashPartitioner(2))
    .saveAsTextFile("output")

    sc.stop()

  }
}
