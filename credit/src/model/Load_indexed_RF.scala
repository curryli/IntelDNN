package model
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
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.QuantileDiscretizer
import scala.collection.mutable.HashMap


object Load_indexed_RF {
 
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
 
    val startTime = System.currentTimeMillis(); 
 
    val rangedir = IntelUtil.varUtil.rangeDir 
     
    var input_dir = rangedir + "idx_withlabel"
    var labeledData = IntelUtil.get_from_HDFS.get_indexed_DF(ss, input_dir).persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    labeledData.show(5)
 
    println("testData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
      
    var vec_data = labeledData
    
//    val udf_transid_type = udf[String, String]{xstr => xstr.substring(0,1)}
//    vec_data = vec_data.filter(udf_transid_type(vec_data("trans_id_filled"))==="S")
     
  
    
    
     //该数组李必须都是doubletype，否则VectorAssembler报错
    val CatVecArr = labeledData.columns.toList.drop(1).dropRight(1).toArray   ///.dropRight(1)
      
      
    val assembler = new VectorAssembler()
      .setInputCols(CatVecArr)
      .setOutputCol("featureVector")
    
    val label_indexer = new StringIndexer()
     .setInputCol("label")
     .setOutputCol("label_idx")
     .fit(vec_data)  
       
      
    val rfClassifier = new RandomForestClassifier()
        .setLabelCol("label_idx")
        .setFeaturesCol("featureVector")
        .setSubsamplingRate(0.2)
        .setFeatureSubsetStrategy("auto")
        .setImpurity("gini")
        .setNumTrees(100)
        .setMaxDepth(10)
//        .setNumTrees(200)
        .setMaxBins(10000)
        .setMinInstancesPerNode(2)
        .setThresholds(Array(100,1))
        //为每个分类设置一个阈值，参数的长度必须和类的个数相等。最终的分类结果会是p/t最大的那个分类，其中p是通过Bayes计算出来的结果，t是阈值。 
        //这对于训练样本严重不均衡的情况尤其重要，比如分类0有200万数据，而分类1有2万数据，此时应用new NaiveBayes().setThresholds(Array(100.0,1.0))    这里t1=100  t2=1
     
      
      val Array(trainingData, testData) = vec_data.randomSplit(Array(0.8, 0.2))  
        
      val pipeline = new Pipeline().setStages(Array(assembler,label_indexer, rfClassifier))
      
      val model = pipeline.fit(trainingData)
      
       
       
      val predictionResult = model.transform(testData)
        
      val eval_result = IntelUtil.funUtil.get_CF_Matrix(predictionResult)
       
     println("Current Precision_P is: " + eval_result.Precision_P)
     println("Current Recall_P is: " + eval_result.Recall_P)
     
     
 
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
   
     
  }
  
  
  
    
}