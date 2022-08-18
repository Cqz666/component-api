package com.cqz.spark.func

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
 * 当透视数据时，需要确定三个要素
 * 1、要在行（分组元素）中看到的元素 graduation_year
 * 2、要在列（扩展元素）上看到的元素  gender
 * 3、要在数据部分看到的元素（聚合元素） weight
 */
object Row2Col {
  case class Student(name:String, gender:String, weight:Int, graduation_year:Int)
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    import spark.implicits._
    val studentsDF = Seq(
      Student("刘宏明", "男", 180, 2015),
      Student("赵薇", "女", 110, 2015),
      Student("黄海波", "男", 200, 2015),
      Student("杨幂", "女", 109, 2015),
      Student("楼一萱", "女", 105, 2015),
      Student("龙梅子", "女", 115, 2016),
      Student("陈知远", "男", 195, 2016)
    ).toDF()
    // 计算每年每个性别的平均体重
    studentsDF.groupBy("graduation_year")
      .pivot("gender")
      .avg("weight")
      .show()

    studentsDF
      .groupBy("graduation_year")
      .pivot("gender")
      .agg(
        min("weight").as("min"),
        max("weight").as("max"),
        avg("weight").as("avg")
      ).show()

    studentsDF
      .groupBy("graduation_year")
      .pivot("gender", Seq("男"))
      .agg(
        min("weight").as("min"),
        max("weight").as("max"),
        avg("weight").as("avg")
      ).show()



  }
}
