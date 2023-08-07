package Spark_Sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Cou_types {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession对象
    val spark = SparkSession.builder()
      .appName("Cou_types")
      .master("local[*]")
      .getOrCreate()

    // 读取数据文件到DataFrame中，由于数据文件的第一行是表头，所以设置header选项为true，同时设置inferSchema选项为true自动推断数据类型
    val filePath = "file:///usr/local/spark/mycode/douban/src/main/scala/movie_info.csv"
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // 使用split函数将类型字段按空格分割成多个字段
    val splitDF = df.withColumn("类型", split(col("类型"), "/"))

    // 将多个类型字段转换成多行
    val explodeDF = splitDF.select(col("ID"), col("电影名称"), col("评分"), col("评价人数"), col("短评数量"), col("影评数量"), col("片长"), col("语言"), explode(col("类型")).alias("类型"), col("导演"), col("制片国家/地区"), col("上映日期"), col("看过人数"), col("想看人数"), col("星级"), col("五星"), col("四星"), col("三星"), col("二星"), col("一星"))

    // 注册表
    explodeDF.createOrReplaceTempView("movie")

    //统计每个类型的平均评分
    val sqlText =
      """
        |SELECT split(`类型`, '/')[0] AS `type`, AVG(`评分`) AS `average_rating`
        |FROM movie
        |GROUP BY split(`类型`, '/')[0]
        |ORDER BY `average_rating` DESC
        |""".stripMargin

    val resultDF = spark.sql(sqlText)

    // 输出结果
    resultDF.show()

    // 将结果保存为CSV文件
    resultDF
      .coalesce(1) // 将数据合并到一个分区中，以便输出到单个文件中
      .write
      .mode("overwrite") // 如果文件已存在，覆盖它
      .option("header", "true") // 将列名写入文件
      .option("delimiter", ",") // 将逗号作为分隔符
      .csv("file:///usr/local/spark/mycode/douban/out/8")

    // 关闭SparkSession对象
    spark.stop()
  }
}

