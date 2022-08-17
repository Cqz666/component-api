package com.cqz.spark.df

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

object DataFrameRow {
  val input = "./common-scala/src/main/resources/people.json"
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()
    val df = spark.read.json(input).persist()
    df.foreach(
      row=>{
        val col1 = row.getAs[String]("name")
        val col2 = row.getAs[Long]("age")
        println(col1 + "\t" + col2)
      }
    )

    df.first().schema.printTreeString()
    //注意字段位置要一致否则匹配不上
    df.collect().map{
      case Row(col1:Long,col2:String) => println(col1 + "\t" + col2)
      case _ => ""
    }
  }
}
