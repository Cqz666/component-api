package com.cqz.spark.df

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SimpleDF {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    val df = spark.range(5).toDF()
    df.printSchema()
    df.show()

    val df1 = spark.range(5).toDF("num")
    df1.show()

    val df2 = spark.range(1,3).toDF("num")
    df2.show()

    val df3 = spark.range(5,15,2).toDF("num")
    df3.show()

    import spark.implicits._
    val seq = Seq(("c1","c2",100L),("a1","a2",200L))
    val seqDF = seq.toDF("col1", "col2", "col3")
    seqDF.printSchema()
    seqDF.show()

  }
}
