package com.cqz.spark.iceberg

import org.apache.iceberg.{ManifestFile, Table}
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.iceberg.spark.{Spark3Util, SparkCatalog}


object RunCombineDataFiles {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf()
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set("spark.sql.catalog.hadoop_catalog","org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.hadoop_catalog.type","hadoop")
    .set("spark.sql.catalog.hadoop_catalog.warehouse","hdfs://qzcs240:9000/tmp/chenqizhu/warehouse/hadoop")
    val sparkSession=SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val table = Spark3Util.loadIcebergTable(sparkSession, "hadoop_catalog.default.gamebox_event_iceberg")

    combineFiles(table)

    deleteSnapshot(table)
  }
  def combineFiles( table: Table): Unit={
    SparkActions
      .get()
      .rewriteDataFiles(table)
      .filter(Expressions.equal("datekey", "20220424"))
      .option("target-file-size-bytes","265289728") // 128MB
      .execute()


  }

  def deleteSnapshot(table: Table): Unit={
    val snapshot=table.currentSnapshot()
    val oldSnapshot=snapshot.timestampMillis() - (1000 * 60 * 60 * 24);
    if (snapshot !=null) {
      table.expireSnapshots().expireOlderThan(oldSnapshot).commit()
    }
  }


}
