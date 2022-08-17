package com.cqz.spark.ds

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SimpleDS {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    import spark.implicits._
    // 从一个简单的集合来创建一个 Dataset
    val ds1 = List.range(1,5).toDS()
    ds1.printSchema()
    ds1.show()
    // 从 RDD 到 Dataset 间的转换，使用类型推断
    val phones = List(
      ("小米","中国",3999.00), ("华为","中国",4999.00), ("苹果","美国",5999.00), ("三星","韩国",1999.00), ("诺基亚","荷兰",999.00)
    )
    val phones_ds = spark.sparkContext.parallelize(phones).toDS()
    phones_ds.printSchema()
    phones_ds.show()

    // 检查数据类型
    phones_ds.dtypes.foreach(println)
    // 查看 schema
    phones_ds.printSchema
    // 检查执行计划
    phones_ds.explain()


  }
}
