package com.cqz.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object JoinTest {
  case class Employee(first_name:String, dept_no:Long)
  case class Dept(id:Long, name:String)
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()
    import spark.implicits._
    val employeeDF = Seq(
      Employee("刘宏明", 31),
      Employee("赵薇", 33),
      Employee("黄海波", 33),
      Employee("杨幂", 34),
      Employee("楼一萱", 34),
      Employee("龙梅子", null.asInstanceOf[Int])
    ).toDF
    val deptDF = Seq(Dept(31, "销售部"), Dept(33, "工程部"), Dept(34, "财务部"), Dept(35, "市场营销部")).toDF

    employeeDF.createOrReplaceTempView("employees")
    deptDF.createOrReplaceTempView("departments")

    val joinExpression = employeeDF.col("dept_no") === deptDF.col("id")
    employeeDF.join(deptDF,joinExpression,"inner").show()

    spark.sql("select * from employees JOIN departments on dept_no == id").show

    // join 表达式的简写版本
    employeeDF.join(deptDF, 'dept_no === 'id).show
    // 在 join transformation 内指定 join 表达式
    employeeDF.join(deptDF, employeeDF.col("dept_no") === deptDF.col("id")).show
    // 使用 where transformation 指定 join 表达式
    employeeDF.join(deptDF).where('dept_no === 'id).show

    // 连接类型既可以是"left_outer"，也可以是"leftouter"
    employeeDF.join(deptDF, 'dept_no === 'id, "left_outer").show
    spark.sql("select * from employees LEFT OUTER JOIN departments on dept_no == id").show
    // 连接类型既可以是"right_outer"，也可以是"rightouter"
    employeeDF.join(deptDF, 'dept_no === 'id, "right_outer").show
    spark.sql("select * from employees RIGHT OUTER JOIN departments on dept_no == id").show
    //全连接
    employeeDF.join(deptDF, 'dept_no === 'id, "outer").show
    spark.sql("select * from employees FULL OUTER JOIN departments on dept_no == id").show
    // 左反连接
    employeeDF.join(deptDF, 'dept_no === 'id, "left_anti").show
    spark.sql("select * from employees LEFT ANTI JOIN departments on dept_no == id").show
    // 左半连接
    employeeDF.join(deptDF, 'dept_no === 'id, "left_semi").show
    spark.sql("select * from employees LEFT SEMI JOIN departments on dept_no == id").show

    // 交叉连接 (又称笛卡尔连接)
    // 使用 crossJoin transformation 并显示该 count
    employeeDF.crossJoin(deptDF).count
    // 显示前 30 行以观察连接后的数据集中所有的行
    spark.sql("select * from employees CROSS JOIN departments").show(30)

    // 输出执行计划以验证 broadcast hash join 策略被使用了
    employeeDF.join(broadcast(deptDF), employeeDF.col("dept_no") === deptDF.col("id")).explain()
    // 使用 SQL
    spark.sql("select /*+ MAPJOIN(departments) */ * from employees JOIN departments on dept_no == id").explain()


  }
}
