package com.cqz.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Rdd2DF {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    val peopleRdd = spark.sparkContext.textFile("D:\\Project\\component-api\\common-scala\\src\\main\\resources\\people.txt")
    val schemaString = "name age"
    val fields: Array[StructField] = schemaString.split(" ").map(filedName => StructField(filedName, StringType, true))
    val schema: StructType = StructType(fields)

    val rowRdd: RDD[Row] = peopleRdd.map(_.split(",")).map(attr => Row(attr(0), attr(1).trim))
    val peopleDF = spark.createDataFrame(rowRdd, schema)

    peopleDF.createOrReplaceTempView("people")
    spark.sql("select * from people").show()

    peopleDF.groupBy(peopleDF("name")).count().show()
    peopleDF.groupBy("name").count().show()


    val toDebugString: String = peopleDF.rdd.toDebugString
    println(toDebugString)

    println(peopleDF.rdd.partitions.size)

    val all = spark.sparkContext.getConf.getAll
    for (elem <- all) {println(elem._1+"=>"+elem._2)}

    spark.close()
  }

}
