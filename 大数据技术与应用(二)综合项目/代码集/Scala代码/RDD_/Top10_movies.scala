package RDD_

import org.apache.spark.{SparkConf, SparkContext}

object Top10_movies {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf对象
    val conf = new SparkConf().setAppName("Top10_movies").setMaster("local[*]")

    // 创建SparkContext对象
    val sc = new SparkContext(conf)

    // 读取数据文件并创建RDD
    val data = sc.textFile("file:///usr/local/spark/mycode/douban/src/main/scala/movie_info.csv")

    // 获取表头
    val header = data.first()

    // 将每一行数据解析为元组，并排除表头
    val parsedData = data.filter(_ != header)
      .map(line => line.split(","))
      .map(fields => (fields(1), fields(2).toDouble))

    // 按照电影评分从高到低排序，并取前十部电影的名称
    val topMovies = parsedData.sortBy(-_._2)
      .take(10).map(tuple => (tuple._2, tuple._1))

    // 输出结果
    println("Top 10 movies by rating:")
    topMovies.foreach(println)

    // 关闭SparkContext对象
    sc.stop()
  }
}
