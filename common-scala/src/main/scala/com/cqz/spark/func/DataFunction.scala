package com.cqz.spark.func

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_add, date_format, date_sub, datediff, dayofmonth, dayofyear, from_unixtime, hour, last_day, minute, month, months_between, next_day, quarter, second, to_date, to_timestamp, unix_timestamp, weekofyear, year}

object DataFunction {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
      .set("spark.sql.legacy.timeParserPolicy","LEGACY")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()
    import spark.implicits._
    // 1）日期和时间转换函数：这些函数使用的默认的日期格式是 yyyy-mm-dd HH:mm:ss
    // 构造一个简单的 DataFrame，注意最后两列不遵循默认日期格式
    val testDate = Seq((1, "2022-01-01", "2022-01-01 15:04:58", "01-01-2022", "12-05-2022 45:50"))
    val testDateTSDF = testDate.toDF("id", "date", "timestamp", "date_str", "ts_str")

    val result = testDateTSDF.select(
      to_date('date).as("date1"),
      to_timestamp('timestamp).as("ts1"),
      to_date('date_str, "MM-dd-yyyy").as("date2"),
      to_timestamp('ts_str, "MM-dd-yyyy mm:ss").as("ts2"),
      unix_timestamp('timestamp).as("unix_ts")
    )

    result.printSchema()
    result.show()

    result.select(
      date_format('date1, "dd-MM-YYYY").as("date_str"),
      date_format('ts1, "dd-MM-YYYY HH:mm:ss").as("ts_str"),
      from_unixtime('unix_ts,"dd-MM-YYYY HH:mm:ss").as("unix_ts_str")
    ).show()

    // 2) 日期-时间（date-time）计算函数
    val employeeData = Seq(
      ("黄渤", "2016-01-01", "2017-10-15"), ("王宝强", "2017-02-06", "2017-12-25")
    ).toDF("name", "join_date", "leave_date")
    employeeData.show()
    // 执行 date 和 month 计算
    employeeData.select(
      'name,
      datediff('leave_date, 'join_date).as("days"),
      months_between('leave_date, 'join_date).as("months"),
      last_day('leave_date).as("last_day_of_mon")
    ).show()
    // 执行日期加、减计算
    val oneDate = Seq("2019-01-01").toDF("new_year")
    oneDate.select(
      date_add('new_year, 14).as("mid_month"),
      date_sub('new_year, 1).as("new_year_eve"),
      next_day('new_year, "Mon").as("next_mon")
    ).show()
    // 转换不规范的日期：
    val df = Seq( "Nov 05, 2018 02:46:47 AM", "Nov 5, 2018 02:46:47 PM").toDF("times")
    df.withColumn("time2",from_unixtime(unix_timestamp($"times","MMM d, yyyy hh:mm:ss a"),"yyyy-MM-dd HH:mm:ss")).show()

    // 3）提取日期或时间戳值的特定字段（如年、月、小时、分钟和秒）
    // 从一个日期值中提取指定的字段
    val valentimeDateDF = Seq("2019-02-14 13:14:52").toDF("date")
    valentimeDateDF.select(
      year('date).as("year"), // 年
      quarter('date).as("quarter"), // 季
      month('date).as("month"), // 月
      weekofyear('date).as("woy"), // 周
      dayofmonth('date).as("dom"), // 日
      dayofyear('date).as("doy"), // 天
      hour('date).as("hour"), // 小时
      minute('date).as("minute"), // 分
      second('date).as("second") // 秒
    ).show()


  }
}
