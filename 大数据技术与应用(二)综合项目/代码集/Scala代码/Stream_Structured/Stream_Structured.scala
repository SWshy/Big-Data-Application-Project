package Stream_Structured

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}

object Stream_Structured {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Stream_Structured ").setMaster("local[*]")

    // 创建SparkSession对象
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // 监听包含CSV文件的目录，并指定具体的文件名
    val csvFilePath = "file:///usr/local/spark/mycode/douban/src/main/scala/"

    val csvStream = spark.readStream
      .format("text")
      .option("maxFilesPerTrigger", 1) // 每次处理一个文件
      .load(csvFilePath)

    import spark.implicits._

    // 数据处理逻辑
    val processedStream = csvStream.selectExpr("value as line")
      .as[String]
      .filter(line => !line.startsWith("ID")) // 过滤掉表头
      .map(line => {
        val fields = line.split(",")
        (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8), fields(9), fields(10), fields(11), fields(12), fields(13), fields(14), fields(15), fields(16), fields(17), fields(18), fields(19))
      })
      .toDF("ID", "电影名称", "评分", "评价人数", "短评数量", "影评数量", "片长", "语言", "类型", "导演", "制片国家/地区", "上映日期", "看过人数", "想看人数", "星级", "五星", "四星", "三星", "二星", "一星")

    processedStream.createOrReplaceTempView("movies")

    // 定义查询逻辑
    //评分大于8的电影的平均获得五星比例和一星比例
    val avgStarDF = spark.sql(
      """
      SELECT AVG(`五星`) AS 5Starts, AVG(`一星`) AS 1Starts
      FROM movies
      WHERE `评分` >= 8
      """)

    //每个类型的总评价人数
    val typeSum_peoDF = spark.sql(
      """
      SELECT type, AVG(`评价人数`) AS avg_peo
      FROM (
      SELECT *, SPLIT(`类型`, '/') AS type_array
      FROM movies
      ) t LATERAL VIEW explode(type_array) AS type
      GROUP BY type
      ORDER BY avg_peo DESC
      """)

    // 输出结果
    val outputDir = "file:///usr/local/spark/mycode/douban/src/main/scala/output"
    val query = avgStarDF.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(ProcessingTime("10 seconds"))// 每10秒触发一次查询
      .start(outputDir)

    val query2 = typeSum_peoDF.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(ProcessingTime("10 seconds"))// 每10秒触发一次查询
      .start()

    // 等待查询完成
    query.awaitTermination()
    query2.awaitTermination()
//    query3.awaitTermination()
  }
}

