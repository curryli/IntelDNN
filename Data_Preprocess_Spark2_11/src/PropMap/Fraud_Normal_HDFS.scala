package PropMap
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


object Fraud_Normal_HDFS {
  

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
     
    val startdate = "20160701"
    val enddate = "20160701"
    var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(5000).persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    AllData.show(5)
    println("AllData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
    var fraud_join_Data = IntelUtil.get_from_HDFS.get_fraud_join_DF(ss, startdate, enddate).repartition(100) 
    fraud_join_Data.show(5)
    println("fraud_join_Data done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
   
    //sys_tra_no, pri_acct_no_conv, mchnt_cd, pdate 
    var FraudData = AllData.join(fraud_join_Data, AllData("sys_tra_no")===fraud_join_Data("sys_tra_no") 
                                               && AllData("pri_acct_no_conv")===fraud_join_Data("pri_acct_no_conv")
                                               && AllData("mchnt_cd")===fraud_join_Data("mchnt_cd")
                                               && AllData("pdate")===fraud_join_Data("pdate"), "left_outer") 
   
    var NormalData = AllData.except(FraudData)
     
  }
  
    
}