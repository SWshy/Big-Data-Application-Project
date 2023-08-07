package RDD_

import org.apache.spark.sql.SparkSession

object Cou_language  {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("Cou_language ")
      .master("local[*]")
      .getOrCreate()

    // 读取数据
    val data = spark.sparkContext.textFile("file:///usr/local/spark/mycode/douban/src/main/scala/movie_info.csv")

    // 获取表头
    val header = data.first()

    // 过滤掉表头
    val filteredData = data.filter(row => row != header)

    // 将每一行数据按照逗号分隔
    val splitData = filteredData.map(row => row.split(","))

    // 提取语言字段
    val languages = splitData.map(row => row(7))

    // 计算每个语言的电影数量
    val languageCount = languages.map(language => (language, 1)).reduceByKey(_ + _)

    // 按数量排序
    val sortedLanguageCount = languageCount.sortBy(-_._2)

    println("每个语言的电影数量:")
    // 输出结果
    sortedLanguageCount.foreach(println)

    // 关闭 SparkSession
    spark.stop()
  }
}
