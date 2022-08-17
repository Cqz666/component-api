package com.cqz.spark.sql

import com.cqz.spark.sql.DataSkewTest.pathA
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataSkewTest2 {
  val path = "D:\\Project\\component-api\\common-scala\\src\\main\\resources\\c.csv"
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    spark.read.csv(path).toDF("id","value").createTempView("a")
    val frame = spark.sql("select id,value,concat(id, '_', cast(round(rand() * 10000) as int)%3) as new_id from a")
    frame.show()
    frame.createTempView("new_a")

    val frame1 = spark.sql("select new_id,sum(value) as cnt from new_a group by new_id")
    frame1.show()
      frame1.createTempView("tmp_table")
    spark.sql("select substring_index(new_id,'_',1) as id, sum(cnt) from tmp_table group by id").show()
  }
}
