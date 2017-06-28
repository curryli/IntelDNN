package Prepare
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
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
 
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD

object test_dummy_result {
 
  val rangedir = IntelUtil.varUtil.rangeDir 
  def main(args: Array[String]): Unit = {

    //屏蔽日志
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR); 
    
    //val sparkConf = new SparkConf().setAppName("spark2SQL")
    val warehouseLocation = "spark-warehouse"
    
    val ss = SparkSession
      .builder()
      .appName("Save_IndexerPipeLine")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.schema.verification", false)
      .getOrCreate()
  
    import ss.implicits._
    import ss.sql
 
    val sc = ss.sparkContext
       
       val parsedData= sc.textFile(rangedir + "dummy_withlabel").map{line=>
           val y = line.split('\t')(1).toDouble
           val x = line.split('\t')(0).split(",").map(_.toDouble)
           LabeledPoint(y, Vectors.dense(x))
       }.cache
 
    
     //测试数据集的作用是评估CV数据集的最好参数
    val Array(trainData, cvData, testData) = parsedData.randomSplit(Array(0.8, 0.1, 0.1))
    trainData.cache()
    cvData.cache()
    testData.cache()

    //构建随机森林
    val model = RandomForest.trainClassifier(trainData, 2, Map[Int, Int](), 20, "auto", "gini", 4, 10000)
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