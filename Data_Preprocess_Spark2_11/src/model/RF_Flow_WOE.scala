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


object RF_Flow_WOE {
  val QD_money_num = 10
  val QD_disc_num = 10
   

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
     
    var input_dir = rangedir + "Labeled_All"
    var labeledData = IntelUtil.get_from_HDFS.get_labeled_DF(ss, input_dir).persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    labeledData.show(5)
 
    println("testData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
      
    var vec_data = labeledData
//    val my_index_Model = PipelineModel.load("xrli/IntelDNN/index_Model")
//    println("Load pipeline done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
//     
//    println("start transform data!")
//    var vec_data = my_index_Model.transform(labeledData)
//    println("Indexed done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
//    vec_data.show(5)
    
    
    val QD_money = new QuantileDiscretizer().setInputCol("trans_at_filled").setOutputCol("trans_at_QD").setNumBuckets(QD_money_num)
    val QD_disc = new QuantileDiscretizer().setInputCol("total_disc_at_filled").setOutputCol("disc_at_QD").setNumBuckets(QD_disc_num)
   
    vec_data = QD_money.fit(vec_data).transform(vec_data)
    vec_data = QD_disc.fit(vec_data).transform(vec_data)
    vec_data.show(5)
    
    val woeMap_trans_at_QD = IntelUtil.funUtil.get_woe_map(vec_data, "trans_at_QD")
    vec_data = IntelUtil.funUtil.WOE_modify(vec_data, "trans_at_QD", woeMap_trans_at_QD)
     
    val woeMap_disc_at_QD = IntelUtil.funUtil.get_woe_map(vec_data, "disc_at_QD")
    vec_data = IntelUtil.funUtil.WOE_modify(vec_data, "disc_at_QD", woeMap_disc_at_QD)
    
    vec_data.show(50)
    
    //val CatVecArr =  Array("trans_at_filled", "total_disc_at_filled")
    //val CatVecArr =  Array("trans_at_QD", "disc_at_QD")
    val CatVecArr =  Array("trans_at_QD_WOE", "disc_at_QD_WOE")
    
    val assembler = new VectorAssembler()
      .setInputCols(CatVecArr)
      .setOutputCol("featureVector")
    
    val label_indexer = new StringIndexer()
     .setInputCol("label_filled")
     .setOutputCol("label_idx")
     .fit(vec_data)  
       
    val rfClassifier = new RandomForestClassifier()
        .setLabelCol("label_idx")
        .setFeaturesCol("featureVector")
        .setNumTrees(5)
        
    
      val Array(trainingData, testData) = vec_data.randomSplit(Array(0.8, 0.2))
  
      val pipeline = new Pipeline().setStages(Array(assembler,label_indexer, rfClassifier))
      
      val model = pipeline.fit(trainingData)
      
      val predictionResultDF = model.transform(testData)
      
      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("label_idx")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")     // spark2 dataframe  支持4种  supports "f1" (default), "weightedPrecision", "weightedRecall", "accuracy")    好像RDD多一点，http://blog.csdn.net/qq_34531825/article/details/52387513?locationNum=4
   
      val predictionAccuracy = evaluator.evaluate(predictionResultDF)
      println("Precision:" +  predictionAccuracy)
      
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  
  
    
}