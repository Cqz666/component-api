package com.cqz.spark.ds

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataSetCreator {
  case class Movie(actor:String,title:String, year:Long)
  val file = "./common-scala/src/main/resources/output/movie-parquet/part-00000-dbb4fbd3-0c14-4336-baff-20f654446f29-c000.snappy.parquet"
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()

    //1. 使用 DataFrame 类的 as（符号）函数将 DataFrame 转换为Dataset；
    val movie = spark.read.parquet(file)
    import spark.implicits._
    // 将 DataFrame 转换到强类型的 Dataset
    val movieDS = movie.as[Movie]
    movieDS.filter(movie=>movie.year==2003).show()

    println(movieDS.first().title)
    // 使用 map transformation 执行投影(projection)
    // map：返回一个新的 Dataset，其中包含对每个元素应用' func '的结果。
    val titleYearDS = movieDS.map(m => (m.title, m.year))
    titleYearDS.printSchema()
    titleYearDS.show()
    // 演示一个类型安全的 transformation-它在编译时失败,因为在字符串类型的列上执行减法// 对于 DataFrame 来说，直到运行时才会检查出问题
//    movie.select('movie_title - 'movie_title).show()
    // 在编译时就能检查出问题
//    movieDS.map(m => m.movie_title - m.movie_title).show()

    movieDS.take(5).foreach(println)

    //2. 使用 SparkSession.createDataset()函数从本地集合对象中创建Dataset
    val localMovies = Seq(
      Movie("郭涛", "疯狂的石头", 2018L), Movie("黄渤", "疯狂的石头", 2018L)
    )
    val localMoviesDS1 = spark.createDataset(localMovies)
    localMoviesDS1.printSchema()
    localMoviesDS1.show()
    //3. 使用 toDS 隐式转换程序
    val localMoviesDS2 = localMovies.toDS()
    localMoviesDS2.printSchema()
    localMoviesDS2.show()



  }
}
