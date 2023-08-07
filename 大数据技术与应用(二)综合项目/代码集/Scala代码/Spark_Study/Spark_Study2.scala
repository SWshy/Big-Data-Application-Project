package Spark_Study

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession

object Spark_Study2 {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Spark Machine Learning")
      .config("spark.master", "local")
      .getOrCreate()

    // 读取CSV数据
    val data = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("file:///usr/local/spark/mycode/douban/src/main/scala/movie_info.csv")

    // 特征工程
    val labelCol = "评分"
    val featureCols = Array("评价人数", "短评数量", "影评数量", "片长", "看过人数", "想看人数")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol(labelCol)
      .setOutputCol("label")
      .setHandleInvalid("skip")

    // 划分训练集和测试集
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // 定义随机森林回归模型
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // 构建Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(assembler, indexer, rf))

    // 训练模型
    val model = pipeline.fit(trainingData)

    // 在测试集上进行预测
    val predictions = model.transform(testData)

    // 模型评估
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)

    // 超参数调优
    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.numTrees, Array(10, 20, 30))
      .addGrid(rf.maxDepth, Array(5, 10, 15))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    val cvModel = cv.fit(data)

    val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel]
    val bestRfModel = bestModel.stages.last.asInstanceOf[RandomForestRegressionModel]
    println("***************************************************************")
    println("Root Mean Squared Error (RMSE) on test data: " + rmse)
    println("***************************************************************")

    println("Best model parameters:")
    println("Num Trees: " + bestRfModel.getNumTrees)
    println("Max Depth: " + bestRfModel.getMaxDepth)
    println("***************************************************************")
  }
}
