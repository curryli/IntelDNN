package PropMap
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}

import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import scala.collection.mutable.MutableList
import scala.Range
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Buffer,Set}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD

object test_dummy_result {
 
 
  def main(args: Array[String]): Unit = {

    //屏蔽日志
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR); 
    
    //val sparkConf = new SparkConf().setAppName("spark2SQL")
    val warehouseLocation = "spark-warehouse"
    
     val conf = new SparkConf().setAppName("compare_2time")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
       val parsedData= sc.textFile("xrli/IntelDNN/Counterfeit/201608/dummy_withlabel").map{line=>
           val y = line.split('\t')(1).toDouble
           val x = line.split('\t')(0).split(",").map(_.toDouble)
           LabeledPoint(y, Vectors.dense(x))
       }.cache
 
    parsedData.map {x => (x.label,x.features)}.take(5).foreach(println)   
    
     //测试数据集的作用是评估CV数据集的最好参数
    val Array(trainData, cvData, testData) = parsedData.randomSplit(Array(0.8, 0.1, 0.1))
    trainData.cache()
    cvData.cache()
    testData.cache()

    //构建随机森林   http://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.mllib.tree.RandomForest$
    val model = RandomForest.trainClassifier(trainData, 2, Map[Int, Int](), 200, "auto", "gini", 30, 5000, scala.util.Random.nextInt(100))
    
    
    
    val metrics = getMetrics(model, cvData)
    println("-----------------------------------------confusionMatrix-----------------------------------------------------")
    //混淆矩阵和模型精确率
    println(metrics.confusionMatrix)
    println("---------------------------------------------precision-------------------------------------------------")
    println(metrics.precision)

    println("-----------------------------------------(precision,recall)---------------------------------------------------")
    //每个类别对应的精确率与召回率
    (0 until 2).map(target => (metrics.precision(target), metrics.recall(target))).foreach(println)
  }
  
  def getMetrics(model: RandomForestModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
    //将交叉验证数据集的每个样本的特征向量交给模型预测,并和原本正确的目标特征组成一个tuple
    val predictionsAndLables = data.map { d =>
      (model.predict(d.features), d.label)
    }
    //将结果交给MulticlassMetrics,其可以以不同的方式计算分配器预测的质量
    new MulticlassMetrics(predictionsAndLables)
  }
  
  
 
  
    
}