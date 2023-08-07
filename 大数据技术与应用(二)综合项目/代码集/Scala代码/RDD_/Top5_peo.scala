package RDD_

import org.apache.spark.{SparkConf, SparkContext}

object Top5_peo {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Top5_peo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 读取数据文件
    val data = sc.textFile("file:///usr/local/spark/mycode/douban/src/main/scala/movie_info.csv")

    // 获取表头
    val header = data.first()

    // 将每一行数据解析为元组，并排除表头
    val parsedData = data.filter(_ != header).map(line => line.split(",")).map(fields => (fields(1), fields(3).toDouble))

    // 按照评价人数从高到低排序，并取前5部电影的名称
    val topMovies = parsedData.sortBy(-_._2).take(5).map(tuple => (tuple._2, tuple._1))

    // 输出结果
    println("评价人数前5的电影:")
    topMovies.foreach(println)

    sc.stop()
  }
}
