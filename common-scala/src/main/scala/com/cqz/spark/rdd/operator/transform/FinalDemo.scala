package com.cqz.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FinalDemo {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)
    val in: RDD[String] = sc.textFile("data/agent.log")
    val mapRdd: RDD[((String, String), Int)] = in.map(
      line => {
        val value: Array[String] = line.split(" ")
        ((value(1), value(4)), 1)
      }
    )
    val reduceRdd: RDD[((String, String), Int)] = mapRdd.reduceByKey(_+_)

    val newRdd: RDD[(String, (String, Int))] = reduceRdd.map {
      case ((pri, ad), sum) => {
        (pri, (ad, sum))
      }
    }
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = newRdd.groupByKey()

    val resultRdd: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    resultRdd.collect().foreach(println)

  }
}
