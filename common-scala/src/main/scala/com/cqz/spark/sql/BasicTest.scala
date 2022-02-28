package com.cqz.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BasicTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    spark.close()
  }
}
