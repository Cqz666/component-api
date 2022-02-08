package com.cqz.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupByOperator {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)

    val in: RDD[String] = sc.makeRDD(List("Hello","Spark","Hi","Scala"))
    val out: RDD[(Char, Iterable[String])] = in.groupBy(_.charAt(0))
    
    out.collect().foreach(println)

    sc.stop()

  }

}
