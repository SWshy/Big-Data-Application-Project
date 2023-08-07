package Stream_Kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import scala.collection.mutable


object Stream_Kafka {
  def main(args: Array[String]): Unit = {
    // 设置Spark配置
    val sparkConf = new SparkConf().setAppName("Stream_Kafka").setMaster("local[*]")
    // 创建StreamingContext，设置批处理间隔为10秒
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))

    // 创建SparkSession对象
    val spark = SparkSession.builder()
      .appName("Stream_Kafka")
      .config(sparkConf)
      .getOrCreate()

    // 设置Kafka相关参数
    val kafkaParams = new mutable.HashMap[String, Object]()
    kafkaParams.put("bootstrap.servers", "localhost:9092")
    kafkaParams.put("key.deserializer", classOf[StringDeserializer])
    kafkaParams.put("value.deserializer", classOf[StringDeserializer])
    kafkaParams.put("group.id", "spark-streaming-consumer-group")
    kafkaParams.put("auto.offset.reset", "latest")
    kafkaParams.put("enable.auto.commit", false: java.lang.Boolean)
    kafkaParams.put("receive.buffer.bytes", 65536: java.lang.Integer)

    val topics = Array("douban") // 指定要订阅的Kafka主题

    // 创建输入DStream，使用Kafka作为数据源
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferBrokers,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // 监测指定目录下的文件，并将文件内容作为消息发送到Kafka
    val directoryPath = "file:///usr/local/spark/mycode/douban/src/main/scala"
    val fileStream = streamingContext.textFileStream(directoryPath)

    fileStream.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        // Kafka生产者配置
        val props = new Properties()
        props.put("bootstrap.servers", "localhost:9092")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)
        partition.foreach { line =>
          val record = new ProducerRecord[String, String]("douban", line)
          producer.send(record)
        }
        producer.close()
      }
    }

    // 数据处理逻辑...
    kafkaStream.foreachRDD((rdd, time) => {
      println("=========================")
      println(rdd)
      if (!rdd.isEmpty()) {
        import spark.implicits._

        val lines = rdd.map(_.value())

        val header = lines.first() // 提取表头
        val data = lines.filter(line => line != header) // 过滤掉表头

        val dataFrame = data.map(line => {
          val fields = line.split(",")
          (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8), fields(9), fields(10), fields(11), fields(12), fields(13), fields(14), fields(15), fields(16), fields(17), fields(18), fields(19))
        }).toDF("ID", "电影名称", "评分", "评价人数", "短评数量", "影评数量", "片长", "语言", "类型", "导演", "制片国家/地区", "上映日期", "看过人数", "想看人数", "星级", "五星", "四星", "三星", "二星", "一星")

        dataFrame.createOrReplaceTempView("movies")

        //获得高星比例前10的电影
        val top10StarDF = spark.sql(
          """
            |SELECT (`五星`+`四星`) AS h, `电影名称`
            |FROM movies
            |ORDER BY h DESC
            |LIMIT 10
            |""".stripMargin)

        // 统计day上映电影数
        val movieCountByDayDF = spark.sql(
          """
            |SELECT DAY(`上映日期`) AS day, COUNT(*) AS count
            |FROM movies
            |GROUP BY DAY(`上映日期`)
            |ORDER BY DAY(`上映日期`)
            |""".stripMargin)

        //影评数量 + 短评数量
        val top10ComDF = spark.sql(
          """
            |SELECT (`影评数量`+`短评数量`) AS result, `电影名称`
            |FROM movies
            |ORDER BY result DESC
            |LIMIT 10
            |""".stripMargin)

        //top10 评价人数的导演
        val top10PeoDF = spark.sql(
          """
            |SELECT  SUM(`评价人数`) AS count, `导演`
            |FROM movies
            |GROUP BY `导演`
            |ORDER BY count DESC
            |LIMIT 10
            |""".stripMargin)

        // 输出结果
        println(s"========= Time: $time =========")
        println("Top10 Hight Star:")
        top10StarDF.show()
        println("Movie Count by Day:")
        movieCountByDayDF.show()
        println("Top10 影评数量 + 短评数量 :")
        top10ComDF.show()
        print("Top10 评价人数的导演:")
        top10PeoDF.show()
      }
    })

    // 启动Streaming应用程序
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
