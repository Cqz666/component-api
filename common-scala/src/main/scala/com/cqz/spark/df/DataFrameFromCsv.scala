package com.cqz.spark.df

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}

object DataFrameFromCsv {
  val file = "./common-scala/src/main/resources/movie.csv"
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()
    val movies = spark.read
      .option("header","true")  //有表头
      .option("inferSchema","true") //自动推断数据类型
      .csv(file)
      .persist()
    movies.printSchema()
    movies.show()

    movies.select("title","year").show(5)

    import spark.implicits._
    movies.select($"title",($"year"-$"year" % 10).as("decade")).show(5)
    movies.selectExpr("*","(year - year % 10) as decade").show(5)

    movies.selectExpr("count(title) as movie","count(actor) as actors").show()

    movies.filter($"year">2000).show()
    movies.where($"year"<=2000).show()

    movies.select("title","year").where('year =!= 2003).show(5)

    movies.filter('year.isin(1995,1998)).show()
    movies.where('year.isin(2001,2003)).show()

    movies.where('year >= 2000 && functions.length('title) < 5).show(5)

    movies.select("title").distinct.selectExpr("count(title) as movies").show()
    movies.dropDuplicates("title").selectExpr("count(title) as movies").show()

    val movieTitles = movies
      .dropDuplicates("title")
      .selectExpr("title", "length(title) as title_length", "year")

    movieTitles.sort("title_length").show()
    movieTitles.sort('title_length.desc).show()

    movieTitles.orderBy('title_length.desc, 'year).show()

    movies.groupBy("year").count()
      .where($"count" > 1)
      .show()

    val shortNameMovieDF = movies.where($"title" === "功夫")
    shortNameMovieDF.show()
    val forgottenActor = Seq(("功夫", 2003L, "赵薇"))
    val forgottenActorDF = forgottenActor.toDF("title","year","actor")
    val completeShortNameMovieDF = shortNameMovieDF.union(forgottenActorDF)
    completeShortNameMovieDF.show()

    movies.withColumn("decade", $"year" - $"year" % 10).show(5)
    movies.withColumn("year", $"year" - $"year" % 10).show(5)

    movies.withColumnRenamed("actor", "actor_name")
      .withColumnRenamed("title", "movie_title")
      .withColumnRenamed("year", "produced_year")
      .show(5)

    movies.drop("actor", "me").printSchema()
    movies.drop("actor", "me").show(5)
    //无放回
    movies.sample(false, 0.0003).show(3)
    //有放回
    movies.sample(true, 0.0003, 123456).show(3)
    //将数据集分割为三部分，比例分别为 0.6、0.3 和 0.1
    val smallerMovieDFs = movies.randomSplit(Array(0.6, 0.3, 0.1))
    smallerMovieDFs(0).show()
    smallerMovieDFs(1).show()
    smallerMovieDFs(2).show()
    // 看看各部分计数之和是否等于 1
    // 数据集总数量
    println(movies.count())
    // 分割后的第 1 个数据集的数量
    println(smallerMovieDFs(0).count())
    // 3 个数据集之和
    println(smallerMovieDFs(0).count() + smallerMovieDFs(1).count() + smallerMovieDFs(2).count() )

    // 返回数据集中第 1 条数据
    println(movies.first())
    // 等价于 first 方法
    println(movies.head())
    // 返回数据集中前 3 条数据，以 Array 形式
    println(movies.head(3).mkString("Array(", ", ", ")"))
    // 返回数据集中前 3 条数据，以 Array 形式
    println(movies.take(3).mkString("Array(", ", ", ")"))
    // 返回数据集中前 3 条数据，以 List 形式
    println(movies.takeAsList(3))
    // 返回一个包含数据集中所有行的数组
    println(movies.collect.mkString("Array(", ", ", ")"))
    // 返回一个包含数据集中所有行的数组，以 List 形式
    println(movies.collectAsList)
    // 返回数据集的数据类型，以 Array 形式
    println(movies.dtypes.mkString("Array(", ", ", ")"))
    // 返回数据集的列名，以 Array 形式
    println(movies.columns.mkString("Array(", ", ", ")"))

    //计算数字列和字符串列的基本统计信息，包括count、mean、stddev、min 和 max
    val descDF = movies.describe()
    descDF.printSchema()
    descDF.show()
    val descDF2 = movies.describe("year")
    descDF2.printSchema()
    descDF2.show()

    val summaryDF = movies.summary()
    summaryDF.printSchema()
    summaryDF.show()
    val summaryDF2 = movies.summary("count", "min", "25%", "75%", "max")
    summaryDF2.printSchema()
    summaryDF2.show()

  }
}
