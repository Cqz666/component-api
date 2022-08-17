package com.cqz.spark.df

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DataFrameFromRdd {
  case class Person(name:String,age:Int)

  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    //1. 使用包含 Row 数据(以元组的形式)的 RDD
    import spark.implicits._
    val tuples = List(("张三", 23), ("李四", 20), ("王无敌", 25))
    val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(tuples)
    val df = rdd.toDF("name", "age")
    df.printSchema()
    df.show()

    //2. 使用 case 类
    val persons = List(Person("A", 1), Person("B", 2), Person("C", 3))
    val value: RDD[Person] = spark.sparkContext.parallelize(persons)
    val personDF = value.toDF()
    personDF.printSchema()
    personDF.show()

    //3. 明确指定一个模式(schema)
    val peopleRDD = spark.sparkContext.parallelize(
      Seq(Row("张三",30), Row("李四",25), Row("王老五",35))
    )
    val fileds = Seq(
      StructField("name",StringType,nullable = true),
      StructField("age",IntegerType,nullable = true)
    )
    val schema: StructType = StructType(fileds)

    val peopleDF = spark.createDataFrame(peopleRDD, schema)
    peopleDF.printSchema()
    peopleDF.show()


  }
}
