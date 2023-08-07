package RDD_

import org.apache.spark.{SparkConf, SparkContext}

object Top5_dir{

  def main(args: Array[String]): Unit = {
    // 创建SparkConf对象，设置应用程序的名称和运行模式
    val conf = new SparkConf().setAppName("Top5_dir").setMaster("local")
    // 创建SparkContext对象
    val sc = new SparkContext(conf)

    // 从指定路径加载CSV文件，并删除表头
    val movieRDD = sc.textFile("file:///usr/local/spark/mycode/douban/src/main/scala/movie_info.csv")
      .mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)
      // 将每一行转换为(导演, (评分, 1))的形式
      .map(line => {
        val fields = line.split(",")
        (fields(9), (fields(2).toDouble, 1))
      })
      // 将同一导演的评分和评分次数相加
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      // 计算每个导演的评分平均值
      .mapValues(x => x._1 / x._2)
      // 将导演按照评分平均值从高到低排序
      .sortBy(_._2, false)


    // 打印出前5个导演及其平均评分
    println("平均评分前5的导演:")
    // 输出前5个导演
    movieRDD.take(5).foreach(println)

    sc.stop() // 关闭SparkContext
  }

}
