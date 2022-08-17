package com.cqz.spark.df

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object DataFrameQuery {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    import spark.implicits._
    val kvDF = Seq((1,2),(3,4)).toDF("key","value").persist()

    kvDF.columns
    // 以不同的方式选择特定的列
    kvDF.select("key").show()     //列为字符串类型
    kvDF.select(col("key")).show()    //col 是内置函数，它返回 Column 类型
    kvDF.select(column("key")).show()   //column 是内置函数，它返回 Column 类型
    kvDF.select(expr("key")).show()   //expr 与 col 方法调用相同
    kvDF.select($"key").show()  //Scala 中构造 Column 类型的语法糖
    kvDF.select('key).show()    //同上

    kvDF.select('key,'key > 1).show   //计算列
    kvDF.select('key,'key > 1 as "aa").show
    // 或（等价）
    kvDF.select('key,('key > 1).alias("aa")).show



  }

}
