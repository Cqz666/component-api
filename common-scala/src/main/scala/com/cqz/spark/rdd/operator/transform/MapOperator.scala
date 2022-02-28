package com.cqz.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapOperator {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)

    val value: RDD[Int] = sc.makeRDD(List(1,2,3,4)).map(_*2)
    val value2: RDD[Int] = sc.makeRDD(List(1,2,3,4)).map((num:Int)=>{num*2})
    value.collect().foreach(println)
    value2.collect().foreach(println)

    sc.stop()

  }

}
