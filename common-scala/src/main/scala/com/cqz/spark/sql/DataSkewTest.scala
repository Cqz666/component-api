package com.cqz.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object DataSkewTest {
  val pathA = "D:\\Project\\component-api\\common-scala\\src\\main\\resources\\a.csv"
  val pathB = "D:\\Project\\component-api\\common-scala\\src\\main\\resources\\b.csv"
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    spark.read.csv(pathA).toDF("id","value").createTempView("a")
    spark.read.csv(pathB).toDF("id","value").createTempView("b")

    spark.sql("select id,value,concat(id, cast(round(rand() * 10000) as int)%3) as new_id from a").createTempView("new_a")
    spark.sql("select * from new_a").show()

    spark.udf.register("newArray",(size: Int)=>{
      val buf = ArrayBuffer[Int]()
      for (a <-0 until  size){
        buf+=a
      }
      buf.toArray
    })
    spark.sql("select newArray(3)").show()

    spark.sql(
      """
        | select id,value,concat(id, suffix) as new_id
        | from (
        |   select id,value, suffix
        |   from b Lateral View explode(newArray(3)) tmp as suffix
        |   )
        |""".stripMargin).createTempView("new_b")
    spark.sql("select * from new_b").show()

    val frame = spark.sql(
      """
        |select a.id,a.value,b.value from new_a  a join new_b b on a.new_id=b.new_id
        |""".stripMargin)
    frame.show()

    spark.close()

  }
}
