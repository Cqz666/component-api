package com.cqz.spark.df

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object WriteCsvDF {
  val file = "./common-scala/src/main/resources/movie.csv"

  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(file)

    val value = df.repartition(4)
    println(value.rdd.getNumPartitions)

    value.write.mode(SaveMode.Ignore).csv("./common-scala/src/main/resources/output/movie-output")

    df.coalesce(1).write.mode(SaveMode.Ignore).csv("./common-scala/src/main/resources/output/movie-output-1")

    import spark.implicits._
    df.repartition($"year")
      .write.partitionBy("year")
      .mode(SaveMode.Ignore)
      .save("./common-scala/src/main/resources/output/movie-part-output")

    df.coalesce(1).write.mode(SaveMode.Ignore).parquet("./common-scala/src/main/resources/output/movie-parquet")


  }
}
