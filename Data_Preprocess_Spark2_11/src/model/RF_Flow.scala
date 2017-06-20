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


object RF_Flow {
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
      
    //val vec_data = labeledData
    val my_index_Model = PipelineModel.load("xrli/IntelDNN/index_Model")
    println("Load pipeline done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
     
    println("start transform data!")
    var vec_data = my_index_Model.transform(labeledData)
    println("Indexed done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
    vec_data.show(5)
    
 
     //该数组李必须都是doubletype，否则VectorAssembler报错
    //val CatVecArr =  Array("day_week_filled","hour_filled","tfr_dt_tm_filled","trans_at_filled", "total_disc_at_filled")
     val CatVecArr =  Array("day_week_filled","hour_filled", "trans_at_filled", "total_disc_at_filled")

    
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
        
    
      val Array(trainingData, testData) = vec_data.randomSplit(Array(0.9, 0.1))
  
      val pipeline = new Pipeline().setStages(Array(assembler,label_indexer, rfClassifier))
      
      val model = pipeline.fit(trainingData)
      
      val predictionResultDF = model.transform(testData)
      
      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("label_idx")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")     // spark2 dataframe  支持4种  supports "f1" (default), "weightedPrecision", "weightedRecall", "accuracy")    好像RDD多一点，http://blog.csdn.net/qq_34531825/article/details/52387513?locationNum=4
   
      val predictionAccuracy = evaluator.evaluate(predictionResultDF)
      println("Precision:" +  predictionAccuracy)
       
      var test_07 = IntelUtil.get_from_HDFS.get_labeled_DF(ss, "xrli/IntelDNN/Counterfeit/201607/Labeled_All") 
      val predictionResultDF_07 = model.transform(test_07)
      
      val predictionAccuracy_07 = evaluator.evaluate(predictionResultDF_07)
      println("Precision_07:" +  predictionAccuracy_07)    
      
      
      

     val TP_Cnt = predictionResultDF_07.filter(predictionResultDF_07("label_idx") === predictionResultDF_07("prediction")).filter(predictionResultDF_07("label_idx")===1).count.toDouble
     val TN_Cnt = predictionResultDF_07.filter(predictionResultDF_07("label_idx") === predictionResultDF_07("prediction")).filter(predictionResultDF_07("label_idx")===0).count.toDouble
     val FP_Cnt = predictionResultDF_07.filter(predictionResultDF_07("label_idx") !== predictionResultDF_07("prediction")).filter(predictionResultDF_07("prediction")===1).count.toDouble
     val FN_Cnt = predictionResultDF_07.filter(predictionResultDF_07("label_idx") !== predictionResultDF_07("prediction")).filter(predictionResultDF_07("prediction")===0).count.toDouble
     println("TP_Cnt is: " + TP_Cnt)
     println("TN_Cnt is: " + TN_Cnt)
     println("FP_Cnt is: " + FP_Cnt)
     println("FN_Cnt is: " + FN_Cnt)
     
     val Precision_P = TP_Cnt/(TP_Cnt + FP_Cnt)
     val Recall_P = TP_Cnt/(TP_Cnt + FN_Cnt)

     println("Current Precision_P is: " + Precision_P)
     println("Current Recall_P is: " + Recall_P)
     
     
     val N_sample_ratio = 80000000/(FP_Cnt + TN_Cnt)   //(FP_Cnt + TN_Cnt)采样后实际为0的个数     80000000是采样前实际为0的个数
     val Precision_P_Actual = TP_Cnt/(TP_Cnt + FP_Cnt*N_sample_ratio)
     val Recall_P_Actual = TP_Cnt/(TP_Cnt + FN_Cnt*N_sample_ratio)

     println("Actual Precision_P is: " + Precision_P_Actual)
     println("Actual Recall_P is: " + Precision_P_Actual)     
      
      
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  
  
    
}