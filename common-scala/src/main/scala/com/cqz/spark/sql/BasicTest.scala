package com.cqz.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BasicTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()
    spark.sql(
      """
        |show databases;
        |""".stripMargin)
    spark.sql(
      """
        |use test;
        |""".stripMargin)
    spark.sql(
      """
        |create table hudi_cow_nonpcf_tbl (
        |  uuid int,
        |  name string,
        |  price double
        |) using hudi;
        |""".stripMargin)
    spark.sql(
      """
        |create table hudi_mor_tbl (
        |  id int,
        |  name string,
        |  price double,
        |  ts bigint
        |) using hudi
        |tblproperties (
        |  type = 'mor',
        |  primaryKey = 'id',
        |  preCombineField = 'ts'
        |);
        |""".stripMargin)
    spark.sql(
      """
        |create table hudi_cow_pt_tbl (
        |  id bigint,
        |  name string,
        |  ts bigint,
        |  dt string,
        |  hh string
        |) using hudi
        |tblproperties (
        |  type = 'cow',
        |  primaryKey = 'id',
        |  preCombineField = 'ts'
        | )
        |partitioned by (dt, hh)
        |location '/tmp/chenqizhu/hudi/hudi_cow_pt_tbl';
        |""".stripMargin)

    spark.sql(
      """
        |create table hudi_ctas_cow_nonpcf_tbl
        |using hudi
        |tblproperties (primaryKey = 'id')
        |as
        |select 1 as id, 'a1' as name, 10 as price;
        |""".stripMargin)



    spark.close()
  }
}
