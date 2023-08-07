package Spark_Study

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

object Spark_Study {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("Spark_Study")
      .master("local[*]")  // 这里使用本地模式，[*] 表示使用所有可用的 CPU 核心
      .getOrCreate()

    // 读取 CSV 文件
    val data = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///usr/local/spark/mycode/douban/src/main/scala/movie_info.csv")

    // 特征工程：将文本特征转换为数值特征
    val labelIndexer = new StringIndexer()
      .setInputCol("星级")
      .setOutputCol("label")

    val assembler = new VectorAssembler()
      .setInputCols(Array("评分", "评价人数", "短评数量", "影评数量", "片长", "看过人数", "想看人数"))
      .setOutputCol("features")

    // 构建分类器模型
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // 创建 Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, assembler, rf))

    // 划分训练集和测试集
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // 使用交叉验证进行超参数调优
    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.numTrees, Array(10, 20, 30))  // 调整决策树数量
      .addGrid(rf.maxDepth, Array(5, 10, 15))  // 调整决策树最大深度
      .build()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val crossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)  // 设置交叉验证的折数

    // 拟合训练集并进行交叉验证
    val cvModel = crossValidator.fit(trainingData)

    // 在测试集上进行预测
    val predictions = cvModel.transform(testData)

    // 模型评估
    val accuracy = evaluator.evaluate(predictions)
    println("*************************************************")
    println(s"Accuracy: $accuracy")

    // 查看最佳模型的参数
    val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[RandomForestClassificationModel]
    println("*************************************************")
    println(s"Best model parameters:\n${bestModel.explainParams()}")
    println("*************************************************")

    // 保存模型
    cvModel.write.overwrite().save("file:///usr/local/spark/mycode/douban/src/main/scala/model")

    // 停止 SparkSession
    spark.stop()
  }
}
