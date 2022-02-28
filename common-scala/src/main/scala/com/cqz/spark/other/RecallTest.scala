package com.cqz.spark.other

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RecallTest {
  def main(args: Array[String]): Unit = {
    val id = args(0)
    val path = args(1)
    println(">>>>>>>data_path:" + path)
    val sc = SparkSession.builder().enableHiveSupport().appName("recall").getOrCreate()

    sc.sql("set hive.parquet_optimized_reader_enabled=true")
    sc.sql("set hive.parquet_predicate_pushdown_enabled = true")

    val sourceRDD: RDD[(String, (String, Double))] =
      sc.read.orc(path).rdd.map(x => (id + ":" + x.getAs[String](0), (x.getAs[String](1), Math.round(x.getAs[Double](2) * 100).toDouble / 100.toDouble)))
    val sourceCount: Long = sourceRDD.count()
    val result = sourceRDD.groupByKey()
      .map(line => {

        //对value的数据进行排序然后进行固化
        var recallStr = ""
        line._2.toArray.sortBy(_._2)(Ordering[Double].reverse).map(x => {
          if (recallStr.length == 0) {
            recallStr += x._1 + ":" + x._2
          } else {
            recallStr += "," + x._1 + ":" + x._2
          }
        }
        )
        val key = line._1
        val value = recallStr
        (key, value)
      })
    println(sourceCount)
    result.collect().foreach(x=>{
      println(x._1+","+x._2)
    })

//    sourceRDD.saveAsTextFile("hdfs:///user/chenqizhu/spark/result1.txt")

//    result.foreachPartition(Iterator=>
//        Iterator.foreach(y=>{
//      println("------------------"+y._1+","+y._2)
//    }))

    sc.stop()
  }


}
