package Stream_RDD

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.{SparkSession, functions}

object Stream_RDD {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Stream_RDD ").setMaster("local[*]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))

    // 创建SparkSession对象
    val spark = SparkSession.builder()
      .appName("Stream_RDD ")
      .config(sparkConf)
      .getOrCreate()

    // 监听包含CSV文件的目录，并指定具体的文件名
    val csvFilePath = "file:///usr/local/spark/mycode/douban/src/main/scala/"

    val csvStream = streamingContext.textFileStream(csvFilePath)

    // 数据处理逻辑
    csvStream.foreachRDD((rdd, time) => {
      println("=========================")
      println(rdd)
      if (!rdd.isEmpty()) {
        import spark.implicits._

        val header = rdd.first() // 提取表头
        val data = rdd.filter(line => line != header) // 过滤掉表头

        val dataFrame = data.map(line => {
          val fields = line.split(",")
          (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8), fields(9), fields(10), fields(11), fields(12), fields(13), fields(14), fields(15), fields(16), fields(17), fields(18), fields(19))
        }).toDF("ID", "电影名称", "评分", "评价人数", "短评数量", "影评数量", "片长", "语言", "类型", "导演", "制片国家/地区", "上映日期", "看过人数", "想看人数", "星级", "五星", "四星", "三星", "二星", "一星")
        dataFrame.createOrReplaceTempView("movies")

        //平均星级
        val avgStarDF = spark.sql(
          """
      SELECT AVG(`星级`) AS avg_star FROM movies
      """)

        // 统计每月上映电影数
        val movieCountByMonthDF = spark.sql(
          """
      SELECT MONTH(`上映日期`) AS release_month, COUNT(*) AS count
      FROM movies
      GROUP BY MONTH(`上映日期`)
      ORDER BY MONTH(`上映日期`)
      """)

        //影评数量/评价人数
        val top10PeoDF = spark.sql(
          """
      SELECT (`影评数量`/`评价人数`) AS result, `电影名称`
      FROM movies
      ORDER BY result DESC
      LIMIT 10
      """)

        // 输出结果
        println(s"========= Time: $time =========")
        println("Average Star:")
        avgStarDF.show()
        println("Movie Count by Month:")
        movieCountByMonthDF.show()
        print("Top10 影评数量 / 评价人数 :")
        top10PeoDF.show()
      }
    })

    // 启动Streaming应用程序
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
