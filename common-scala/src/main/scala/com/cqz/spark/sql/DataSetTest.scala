package com.cqz.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object DataSetTest {

  case class Person(name:String, age:Long)

  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()
    import spark.implicits._

    val ds = Seq(Person("andy", 32), Person("amy", 23)).toDS()
    ds.show()

    val ds2 = Seq(1, 2, 3).toDS()
    ds2.map(_ +1).toDF().show()

    val path = "D:\\Project\\component-api\\common-scala\\src\\main\\resources\\people.json"
    val personDs = spark.read.json(path).as[Person]
    personDs.show()


  }
}
