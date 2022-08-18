package com.cqz.spark.func

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, concat_ws, sort_array}

object Data2Demo {
  val path = "./common-scala/src/main/resources/data2.csv"
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    val input = spark.read.option("inferSchema", "true").option("header", "false").csv(path).toDF("year","month","cnt")
    input.show()
    import spark.implicits._
    val df = input.select(concat_ws("-", $"year", $"month").as("ym"), $"cnt")
    df.show()

    df.groupBy("ym")
      .agg(sort_array(collect_list($"cnt")).as("cnt"))
      .orderBy("ym")
      .show()




  }
}
