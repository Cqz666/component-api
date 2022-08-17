package com.cqz.spark.ds

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.javalang.typed

object DataSetOperator {
  case class Product(title: String, quantity: Int, price: Double)
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    import spark.implicits._
    val products = Seq(
      Product("服装", 2, 2000.00),
      Product("服装", 1, 2500.00),
      Product("玩具", 3, 500.00),
      Product("玩具", 2, 500.00),
      Product("玩具", 4, 1000.00)
    )
    val ds = products.toDS().cache()
    ds.show()

    // 简单分组统计
    ds.groupByKey(_.title).count().show()

    //同时统计多列
    ds.groupByKey(_.title)
      .agg(
        typed.sum[Product](_.quantity), // 总数量
        typed.avg[Product](_.price),  // 均价
//        typed.count(_.title)   // 类别数
      )
      .toDF("商品","总量","均价")
      .orderBy($"value".desc)
      .show()

  }
}
