package com.cqz.spark.df

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ShufflePartition {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions",100)
    val simpleData = Seq(
      ("张三","销售部","北京",90000,34,10000), ("李四","销售部","北京",86000,56,20000), ("王老五","销售部","上海",81000,30,23000), ("赵老六","财务部","上海",90000,24,23000), ("钱小七","财务部","上海",99000,40,24000), ("周扒皮","财务部","北京",83000,36,19000), ("孙悟空","财务部","北京",79000,53,15000), ("朱八戒","市场部","上海",80000,25,18000), ("沙悟净","市场部","北京",91000,50,21000)
    )
    import spark.implicits._
    val df = simpleData.toDF("employee_name","department","city","salary","age","bonus")
    println(s"shuffle 前的分区数：${df.rdd.getNumPartitions}")
    // groupBy 操作会触发数据 shuffle
    val df2 = df.groupBy("city").count()
    println(s"shuffle 后的分区数：${df2.rdd.getNumPartitions}")
  }
}
