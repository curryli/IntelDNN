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


object Label_noSample {
    val startdate = IntelUtil.varUtil.startdate
    val enddate = IntelUtil.varUtil.enddate
    val rangedir = IntelUtil.varUtil.rangeDir 
    val usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
     
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
    
     var counterfeit_filled = IntelUtil.get_from_HDFS.get_processed_DF(ss, rangedir + "counterfeit_filled")
     val udf_substring = udf[String, String]{xstr => xstr.substring(0,4)}
     counterfeit_filled = counterfeit_filled.filter(udf_substring(counterfeit_filled("tfr_dt_tm_filled"))=== "0701")
     println("counterfeit_filled count is " + counterfeit_filled.count()) 

     var fraud_join_Data = IntelUtil.get_from_HDFS.get_fraud_join_DF(ss, startdate, enddate).persist(StorageLevel.MEMORY_AND_DISK_SER)
     println("fraud_join_Data done")    
     
     var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(3000)  
     println("AllData done")    
     
     AllData = AllData.selectExpr(usedArr_filled:_*)//.persist(StorageLevel.MEMORY_AND_DISK_SER)
     println("AllData select done") 
     
     AllData.show(2)
     fraud_join_Data.show(2)
     
     var normaldata_filled = AllData.join(fraud_join_Data, AllData("pri_acct_no_conv_filled")=!=fraud_join_Data("pri_acct_no_conv"), "leftsemi")
     println("normaldata_filled count is " + normaldata_filled.count()) 
     
     //AllData.unpersist(false)
     fraud_join_Data.unpersist(false) 
      
     val udf_Map0 = udf[Double, String]{xstr => 0.0}
     val udf_Map1 = udf[Double, String]{xstr => 1.0}
      
     var NormalData_labeled = normaldata_filled.withColumn("isFraud", udf_Map0(normaldata_filled("trans_md_filled")))
     var counterfeit_labeled = counterfeit_filled.withColumn("isFraud", udf_Map1(counterfeit_filled("trans_md_filled")))
     var LabeledData = counterfeit_labeled.unionAll(NormalData_labeled)
     LabeledData.show(50) 
     
    
      
     LabeledData.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "Label_noSample_0701")
    
    println("step1 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
      
     
  }
    
 
      
  
    
}