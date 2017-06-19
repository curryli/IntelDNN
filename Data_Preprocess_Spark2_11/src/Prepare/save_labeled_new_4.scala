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


object save_labeled_new_4 {
  

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
 
    val startTime = System.currentTimeMillis(); 
      
    val startdate = IntelUtil.varUtil.startdate
    val enddate = IntelUtil.varUtil.enddate
    val rangedir = IntelUtil.varUtil.rangeDir  
     
    var usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
    
    val counterfeit_cards= sc.textFile(rangedir + "counterfeit_cards").cache
    val counterfeit_cards_list = counterfeit_cards.collect()
    
    val sample_cards= sc.textFile(rangedir + "All_sample_cards").cache
    val all_cards = sample_cards.union(counterfeit_cards)
    val all_cards_list = all_cards.collect()
   
    val Alldata_by_cards = sc.textFile(rangedir + "Alldata_by_cards").map{str=>
           var tmparr = str.split(",")         
           tmparr = tmparr.map { x => x.toString()}    
           Row.fromSeq(tmparr.toSeq)
       }
    
    val counterfeit = sc.textFile(rangedir + "counterfeit_filled").map{str=>
           var tmparr = str.split(",")         
           tmparr = tmparr.map { x => x.toString()}    
           Row.fromSeq(tmparr.toSeq)
       }
    
    var counterfeit_filled = ss.createDataFrame(counterfeit, IntelUtil.constUtil.schema_used)
    var Alldata_by_cards_filled = ss.createDataFrame(Alldata_by_cards, IntelUtil.constUtil.schema_used)
    
    println("Alldata_by_cards_filled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
  
    var normaldata_filled = Alldata_by_cards_filled.except(counterfeit_filled)
     
     
    var counterfeit_related_all_data = Alldata_by_cards_filled.filter(Alldata_by_cards_filled("pri_acct_no_conv_filled").isin(counterfeit_cards_list:_*))
    println("counterfeit_related_fraud_data count is " + counterfeit_filled.count) 
    println("counterfeit_related_all_data count is " + counterfeit_related_all_data.count) 
    println("normaldata_filled count is " + normaldata_filled.count) 
    
    val udf_Map0 = udf[Double, String]{xstr => 0.0}
    val udf_Map1 = udf[Double, String]{xstr => 1.0}
     
    var NormalData_labeled = normaldata_filled.withColumn("isFraud", udf_Map0(normaldata_filled("trans_md_filled")))
    var counterfeit_labeled = counterfeit_filled.withColumn("isFraud", udf_Map1(counterfeit_filled("trans_md_filled")))
    var LabeledData = counterfeit_labeled.unionAll(NormalData_labeled)
    LabeledData.show(5)
        
         
    LabeledData.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "Labeled_All")
        
     
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
    
    
  }
  
    
}