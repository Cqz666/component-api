package com.cqz.spark.func

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object MathFunction {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    import spark.implicits._
    val numberDF = Seq((3.14159, -3.14159)).toDF("pie", "-pie")
    numberDF
      .select(
        $"pie",
        round($"pie").as("pie0"), // 整数四舍五入
        round($"pie", 2).as("pie1"), // 四舍五入，保留小数点后 2 位
        round($"pie", 4).as("pie2"), // 四舍五入，保留小数点后 4 位
        $"-pie",
        round($"-pie").as("-pie0"), // 整数四舍五入
        round($"-pie", 2).as("-pie1"), // 四舍五入，保留小数点后 2 位
        round($"-pie", 4).as("-pie2") // 四舍五入，保留小数点后 4 位
      ).show()
    numberDF.select(
        $"pie",
        ceil($"pie"), // 向上取整
        floor($"pie"), // 向下取整
        $"-pie",
        ceil($"-pie"), // 向上取整
        floor($"-pie") // 向下取整
      ).show()


  }
}
