package com.cqz.spark.func

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{concat_ws, initcap, lower, lpad, ltrim, regexp_extract, regexp_replace, rpad, rtrim, translate, upper}
import org.apache.spark.sql.{SparkSession, functions}

object StringFunction {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()
    import spark.implicits._

    // 使用各种内置字符串函数转换字符串的各种方法。
    val sparkDF = Seq(" Spark ").toDF("name")
    // 去空格
    sparkDF.select(
      functions.trim($"name").as("trim"), // 去掉"name"列两侧的空格
      ltrim($"name").as("ltrim"), // 去掉"name"列左侧的空格
      rtrim($"name").as("rtrim") // 去掉"name"列右侧的空格
    ).show()
    // 首先去掉"Spark"前后的空格，然后填充到 8 个字符长
    sparkDF.select(functions.trim($"name").as("trim")) // 去掉两侧的空格
      .select(
        lpad($"trim", 8, "-").as("lpad"), // 宽度为 8，不够的话，左侧填充"-"
        rpad($"trim", 8, "=").as("rpad") // 宽度为 8，不够的话，右侧填充"="
      ).show()

    // 使用 concatenation, uppercase, lowercase 和 reverse 转换一个字符串
    val sentenceDF = Seq(("Spark", "is", "excellent")).toDF("subject", "verb", "adj")
    sentenceDF
      .select(concat_ws(" ", $"subject", $"verb", $"adj").as("sentence"))
      .select(
        lower($"sentence").as("lower"),
        upper($"sentence").as("upper"), // 转大写
        initcap($"sentence").as("initcap"), // 转首字母大写
        functions.reverse($"sentence").as("reverse") // 反转
      ).show()

    // 从一个字符转换到另一个字符
    sentenceDF.select($"subject", translate($"subject", "pr", "oc").as("translate")).show()

    // 使用 regexp_extract 字符串函数来提取"fox"，使用一个模式
    val strDF = Seq("A fox saw a crow sitting on a tree singing \"Caw! Caw! Caw!\"").toDF("comment")
    // 使用一个模式
    strDF.select(regexp_extract($"comment", "[a-z]*o[xw]",0).as("substring")).show()
    // 下面两行产生相同的输出
    strDF.select(regexp_replace($"comment", "fox|crow", "animal").as("new_comment")).show(false)
    strDF.select(regexp_replace($"comment", "[a-z]*o[xw]", "animal").as("new_comment")).show(false)

    val telDF = Seq("135a-123b4-c5678").toDF("tel")
    telDF.withColumn("phone",regexp_replace('tel,"-|\\D","")).show()

  }
}
