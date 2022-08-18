package com.cqz.spark.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object UdfDemo {
  case class Student(name:String, score:Int)
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    import spark.implicits._
    val studentDF = Seq(Student("张三", 85), Student("李四", 90), Student("王老五", 55)).toDF()
    studentDF.createOrReplaceTempView("students")
    spark.sql("select * from students").show()

    def convertGrade(score:Int): String ={
      score match {
        case _ if score > 100 =>"作弊"
        case _ if score >= 90 => "优秀"
        case _ if score >= 80 => "良好"
        case _ if score >= 70 => "中等"
        case _ => "不及格"
      }
    }
    val convertGradeUDF = udf(convertGrade(_: Int): String)
    studentDF.select($"name",$"score", convertGradeUDF($"score").as("grade")).show()

  }
}
