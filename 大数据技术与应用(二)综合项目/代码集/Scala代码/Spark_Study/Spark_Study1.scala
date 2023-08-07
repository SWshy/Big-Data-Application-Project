package Spark_Study

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

object Spark_Study1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark_Study1")
      .config("spark.master", "local")
      .getOrCreate()

    // 读取CSV数据
    val data = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("file:///usr/local/spark/mycode/douban/src/main/scala/movie_info.csv")

    // 选择需要的特征列
    val featureCols = Array("评分", "评价人数", "短评数量", "影评数量", "片长", "看过人数", "想看人数", "星级", "五星", "四星", "三星", "二星", "一星")
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val transformedData = assembler.transform(data).cache()  // 缓存输入数据

    // 超参数调优示例
    val kValues = Array(2, 3, 4, 5)
    val seedValues = Array(1L, 2L, 3L)
    var bestWCSS = Double.MaxValue
    var bestK = -1
    var bestSeed = -1L

    for (k <- kValues; seed <- seedValues) {
      val kmeans = new KMeans()
        .setK(k)
        .setSeed(seed)
      val model = kmeans.fit(transformedData)
      val WCSS = model.computeCost(transformedData)
      println(s"对于k=$k 和 seed=$seed，WCSS=$WCSS")
      if (WCSS < bestWCSS) {
        bestWCSS = WCSS
        bestK = k
        bestSeed = seed
      }
    }

    println(s"最佳WCSS=$bestWCSS，最佳k=$bestK，最佳seed=$bestSeed")
    transformedData.unpersist()  // 解除缓存

    spark.stop()
  }
}
