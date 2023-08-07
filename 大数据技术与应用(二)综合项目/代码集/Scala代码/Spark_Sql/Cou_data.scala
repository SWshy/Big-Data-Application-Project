package Spark_Sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Cou_data {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder().appName("Cou_data").master("local[*]").getOrCreate()

    // 读取CSV文件
    val movieDF: DataFrame = spark.read
      .option("header", "true")  // 数据文件中有表头
      .option("inferSchema", "true")  // 自动推断数据类型
      .option("encoding", "UTF-8")  // 指定字符编码
      .csv("file:///usr/local/spark/mycode/douban/src/main/scala/movie_info.csv")

    // 将上映日期转换为日期类型
    val dateDF: DataFrame = movieDF.select(col("ID"), col("电影名称"), col("评分"), col("评价人数"), col("短评数量"),
      col("影评数量"), col("片长"), col("语言"), col("类型"), col("导演"), col("制片国家/地区"),
      from_unixtime(unix_timestamp(col("上映日期"), "yyyy-MM-dd")).as("上映日期"), col("看过人数"), col("想看人数"),
      col("星级"), col("五星"), col("四星"), col("三星"), col("二星"), col("一星"))

    // 注册DataFrame为一个临时表，以便使用Spark SQL进行查询
    dateDF.createOrReplaceTempView("movie")

    // 使用Spark SQL查询每一年的电影数量，并按照年份升序排序
    val resultDF: DataFrame = spark.sql(
      """
        |SELECT YEAR(`上映日期`) AS year, COUNT(*) AS count
        |FROM movie
        |WHERE `上映日期` IS NOT NULL
        |GROUP BY YEAR(`上映日期`)
        |ORDER BY year DESC
      """.stripMargin)

    // 打印查询结果
    resultDF.show()

    // 将结果保存为CSV文件
    resultDF
      .coalesce(1) // 将数据合并到一个分区中，以便输出到单个文件中
      .write
      .mode("overwrite") // 如果文件已存在，覆盖它
      .option("header", "true") // 将列名写入文件
      .option("delimiter", ",") // 将逗号作为分隔符
      .csv("file:///usr/local/spark/mycode/douban/out/7")

    // 关闭SparkSession
    spark.close()
  }
}
