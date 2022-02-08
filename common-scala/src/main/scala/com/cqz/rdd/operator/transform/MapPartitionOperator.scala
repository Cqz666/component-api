package com.cqz.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MapPartitionOperator {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(sparkconf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val mprdd: RDD[Int] = rdd.mapPartitions(
      iter => {
        println(">>>>>")
        iter.map(_ * 2)
      }
    )

    val value: RDD[Int] = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
    value.collect.foreach(println)

//    mprdd.collect().foreach(println)

    var i = rdd.mapPartitionsWithIndex(
      (index,iter)=>{
        if (index==1){
          iter
        }else{
          Nil.iterator
        }
      }
    )
    println("-------------------")
    i.collect.foreach(println)

    var i2 = rdd.mapPartitionsWithIndex(
      (idx,iter)=>{
        iter.map(
          num=>{
            (idx,num)
          }
        )
      }
    )
    println("-------------------")
    i2.collect.foreach(println)


    sc.stop()
  }

}
