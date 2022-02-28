package com.cqz.flink.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Schema}
import org.apache.flink.api.scala._

object IntervalJoin {
  def main(args: Array[String]): Unit = {

    val fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    fsEnv.setParallelism(1)
    val fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings)
    val show = fsEnv.fromElements[JoinInfo](JoinInfo("key_a", "show", 123L),
      JoinInfo("key_b", "show", 124L),
      JoinInfo("key_c", "show", 125L),
      JoinInfo("key_e", "show", 126L),
      JoinInfo("key_f", "show", 127L),
      JoinInfo("key_d","show", 128L),
      JoinInfo("key_h", "show", 128L),
      JoinInfo("key_f", "show", 129L),
      JoinInfo("key_g", "show", 130L),
      JoinInfo("key_show","show", 131L))
      .keyBy(item => item.colA)
    //      .keyBy(elem => elem._1)

    val click = fsEnv.fromElements(JoinInfo("key_a", "click", 126L),
      JoinInfo("key_b", "click", 127L),
      JoinInfo("key_c", "click", 128L),
      JoinInfo("key_e", "click", 129L),
      JoinInfo("key_f", "click", 130L),
      JoinInfo("key_d", "click", 131L),
      JoinInfo("key_h", "click", 132L),
      JoinInfo("key_f", "click", 133L),
      JoinInfo("key_g", "click", 134L),
      JoinInfo("key_click", "click", 135L))
      //      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem.colA)

    val schema: Schema = Schema.newBuilder()
      .column("colA", "STRING")
      .column("colB", "String")
      .column("colC", "BIGINT")
      .columnByExpression("ts", "TO_TIMESTAMP(FROM_UNIXTIME(colC , 'yyyy-MM-dd HH:mm:ss'))")
//      .watermark("ts", "ts - INTERVAL '5' SECOND")
      .build()

    //    val tableA = fsTableEnv.fromDataStream(dataStream1, schema)
    //    val tableB = fsTableEnv.fromDataStream(dataStream2, schema)
    fsTableEnv.createTemporaryView("dclick", click, schema)
    fsTableEnv.createTemporaryView("dshow", show, schema)
    //    fsTableEnv.createTemporaryView(dataStream2,"cola","colb","ts".rowtime)

    val joinT = fsTableEnv.sqlQuery(
      """
        |select dshow.colA,dshow.colB,dshow.colC,
        |dclick.colA,dclick.colB,dclick.colC
        | from
        | --dshow right outer join dclick
        | dclick left outer join dshow
        | on
        |  dshow.colA = dclick.colA
        |and dclick.ts BETWEEN dshow.ts - INTERVAL '10' MINUTE AND
        | dshow.ts + INTERVAL '10' MINUTE
        |""".stripMargin)

    joinT.execute().print()
  }
}
