package com.cqz.spark.df

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}

object DataFrameFromJson {
  val path = "./common-scala/src/main/resources/people.json"
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    val fields = Seq(
      StructField("name",StringType,nullable = true),
      StructField("age",IntegerType,nullable = true)
    )
    val df = spark.read.option("mode", "failFast")  //PERMISSIVE null failFast fail
      .schema(StructType(fields))
      .json(path)
    df.printSchema()
    df.show()


  }

}
