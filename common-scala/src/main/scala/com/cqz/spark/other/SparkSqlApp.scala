package com.cqz.spark.other

import java.io.File

import org.apache.spark.sql.SparkSession

object SparkSqlApp {

  case class Record(key: Int, value: String)

  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("data"+File.separator+"spark-warehouse").getAbsolutePath
    println(warehouseLocation)
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("CREATE TABLE IF NOT EXISTS person (id int, name string, age int) row format delimited fields terminated by ' '")
    spark.sql("LOAD DATA LOCAL INPATH '/home/cqz/IdeaProjects/component-api/common-scala/src/main/resources/kv.txt' INTO TABLE person")
    spark.sql("select * from person ").show()


    spark.stop()

  }
}
