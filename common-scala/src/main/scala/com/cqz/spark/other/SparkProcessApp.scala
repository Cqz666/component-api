package com.cqz.spark.other

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkProcessApp {
  val path = "/home/cqz/IdeaProjects/component-api/common-scala/src/main/resources/data.csv"
  val id = 10
  def main(args: Array[String]): Unit = {
    val sparkContext = SparkSession.builder()
      .master("local")
//      .enableHiveSupport()
      .appName("test")
      .getOrCreate()

    val sourceRdd:RDD[(String,(String,String))] =
      sparkContext.read.csv(path)
        .rdd.map(x => (id + ":" + x.getAs[String](0), (x.getAs[String](1), x.getAs[String](2))))

    val sourceCount = sourceRdd.count()
    val result = sourceRdd.groupByKey()
      .map(line => {
        var value = ""
        line._2.toArray.sortBy(_._2)(Ordering[String].reverse)
          .foreach(x => {
            if (value.length == 0) {
              value += x._1 + ":" + x._2
            } else {
              value += "," + x._1 + ":" + x._2
            }
          })
        val key = line._1
        println(key)
        println(value)

        (key, value)
      })

    println(result)

    sparkContext.stop()
  }

}
