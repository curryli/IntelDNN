package stat
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


object count_fraud_Ratio {
    val startdate = "20160930"
    val enddate = "20160930"
    val rangedir = IntelUtil.varUtil.rangeDir 
    val usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
    
    //可以调整
    val sample_cards_ratio = 0.0005
    val TF_ratio = 500
    val fraudType = "04"
    
    
    var fraudType_cards_num = 0L
    var normal_cards_num = 0L
    var fraudType_related_fraud_count = 0L
     
 
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
    
    save_fraudType_1(ss)
   
    
     
  }
    
    
    def save_fraudType_1(ss: SparkSession): Unit ={
        var fraud_join_Data = IntelUtil.get_from_HDFS.get_fraud_join_DF(ss, startdate, enddate).persist(StorageLevel.MEMORY_AND_DISK_SER)
        var fraudType_infraud = fraud_join_Data.filter(fraud_join_Data("fraud_tp")=== fraudType) 
        var cnt = fraudType_infraud.count()
        println("weika count in week: ", fraudType_infraud.count())
//        var fraud_ratio = 80000000/(cnt/7)
//        println("approximate weika fraud_ratio: ", fraud_ratio)
        
        var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000)
        var all_cnt = AllData.count
        println("all_cnt: ", all_cnt)   //192623
        
        
        var real_ratio = all_cnt/cnt
        println("Real weika fraud_ratio: ", real_ratio)   //9月1~7号  192623      9月30号147128
         
    }
    
     
}