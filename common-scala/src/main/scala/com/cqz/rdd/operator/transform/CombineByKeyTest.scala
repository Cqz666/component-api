package com.cqz.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyTest {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)
    val in: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
        ("b", 4), ("b", 5), ("a", 6)
    ),2)
    val tmp: RDD[(String, (Int, Int))] = in.combineByKey(
      v=>{
        (v,1)
      },
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1,t2)=>{
        (t1._1+t2._1,t1._2+t2._2)
      }
    )
    val out: RDD[(String, Int)] = tmp.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    out.collect().foreach(println)

  }
}
