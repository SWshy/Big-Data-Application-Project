package Spark_Sql

import org.apache.spark.sql.SparkSession

object Top5_len_dir {
  def main(args: Array[String]): Unit = {
  // 创建SparkSession对象
  val spark = SparkSession.builder()
  .appName("Top5_len_dir")
  .master("local[*]")
  .getOrCreate()

  // 读取数据文件到DataFrame中，由于数据文件的第一行是表头，所以设置header选项为true，同时设置inferSchema选项为true自动推断数据类型
  val filePath = "file:///usr/local/spark/mycode/douban/src/main/scala/movie_info.csv"
  val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(filePath)

  // 将DataFrame注册为名为movies的临时表
  df.createOrReplaceTempView("movies")

  // 使用SQL语句统计总片长前5的导演。查询结果按照片长总和降序排列，并取前5个
  val sql =
  """
    |SELECT `导演`, SUM(`片长`) AS rating_sum
    |FROM movies
    |GROUP BY `导演`
    |ORDER BY rating_sum DESC
    |LIMIT 5
  """.stripMargin

  // 执行SQL语句并获取查询结果
  val result = spark.sql(sql)

  // 将查询结果展示在控制台上
  result.show()

    // 将结果保存为CSV文件
    result
      .coalesce(1) // 将数据合并到一个分区中，以便输出到单个文件中
      .write
      .mode("overwrite") // 如果文件已存在，覆盖它
      .option("header", "true") // 将列名写入文件
      .option("delimiter", ",") // 将逗号作为分隔符
      .csv("file:///usr/local/spark/mycode/douban/out/2")

  // 关闭SparkSession对象
  spark.stop()
}
}
