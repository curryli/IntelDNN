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


object save_counterfeit_1 {
  

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
      
    val startdate = IntelUtil.varUtil.startdate
    val enddate = IntelUtil.varUtil.enddate
    val rangedir = IntelUtil.varUtil.rangeDir  
    
    var usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
    
       
    var fraud_join_Data = IntelUtil.get_from_HDFS.get_fraud_join_DF(ss, startdate, enddate).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //fraud_join_Data.show(5)
    println("fraud_join_Data done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
     
  
  
    var counterfeit_infraud = fraud_join_Data.filter(fraud_join_Data("fraud_tp")==="04") 
    
    var counterfeit_cards = counterfeit_infraud.select("pri_acct_no_conv").distinct().persist(StorageLevel.MEMORY_AND_DISK_SER) 
    println("counterfeit_cards count is " + counterfeit_cards.count())
    //counterfeit_cards.show(5)
    counterfeit_cards.rdd.map(r=>r.getString(0)).saveAsTextFile(rangedir + "counterfeit_cards")
 
	  fraud_join_Data.unpersist(false)
	  
	  var counterfeit_cards_list = counterfeit_cards.rdd.map(r=>r.getString(0)).collect()
	
    var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000) 
    println("AllData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
    val counterfeit_related = AllData.filter(AllData("pri_acct_no_conv").isin(counterfeit_cards_list:_*))
    println("counterfeit_related count is " + counterfeit_related.count()) 
    println("counterfeit_related done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
    var counterfeit_fraud = counterfeit_related.join(counterfeit_infraud, counterfeit_related("sys_tra_no")===counterfeit_infraud("sys_tra_no"), "leftsemi")
    
    val counterfeit_filled = counterfeit_fraud.selectExpr(usedArr_filled:_*)
    println("counterfeit_filled count is " + counterfeit_filled.count())
    println("counterfeit_filled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
     
    counterfeit_filled.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "counterfeit_filled")
  
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
    
    
  }
  
    
}