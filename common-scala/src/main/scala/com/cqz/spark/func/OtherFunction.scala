package com.cqz.spark.func

import com.cqz.spark.ds.DataSetCreator.Movie
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object OtherFunction {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    import spark.implicits._
    // 首先生成一个 DataFrame，它的值分散到 5 个分区中
    val numDF = spark.range(1,11,1,5)
    // 验证的确有 5 个分区
    println("分区数为：" + numDF.rdd.getNumPartitions)
    // 现在生成单调递增的值，并查看所在的分区
    import org.apache.spark.sql.functions._
    numDF.select(
      $"id",
      monotonically_increasing_id().as("m_ii"),
      spark_partition_id().as("partition")
    ).show()

    // 创建一个具有从 1 到 7 的值的 DataFrame 来表示一周中的每一天
    val dayOfWeekDF = spark.range(1,8)
    // 将每个数值转换成字符串
    dayOfWeekDF.select(
      $"id",
        when($"id" === 1, "星期一")
        .when($"id" === 2, "星期二")
        .when($"id" === 3, "星期三")
        .when($"id" === 4, "星期四")
        .when($"id" === 5, "星期五")
        .when($"id" === 6, "星期六")
        .when($"id" === 7, "星期七")
        .as("星期")
    ).show()

    dayOfWeekDF.select(
      $"id",
        when($"id" === 6, "周末")
        .when($"id" === 7, "周末")
        .otherwise("工作日")
        .as("day_type")
    ).show()

    // 构造一个 DataFrame，带有 null 值
    val badMoviesDF = Seq(Movie(null, null, 2018L), Movie("黄渤", "一出好戏", 2018L)).toDF()
    badMoviesDF.show()
    // 使用 coalese 来处理 title 列中的 null 值
    badMoviesDF
      .select(
        coalesce($"actor",lit("路人甲")).as("演员"),
        coalesce($"title", lit("烂片")).as("电影"),
        coalesce($"year", lit("2020")).as("年份")
      )
      .show()

  }
}
