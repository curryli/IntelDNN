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


object Save_IndexerPipeLine {
   val startdate = IntelUtil.varUtil.startdate
   val enddate = IntelUtil.varUtil.enddate
   val rangedir = IntelUtil.varUtil.rangeDir 
   var idx_modelname = IntelUtil.varUtil.idx_model
   

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
     
//    var Data_0701 = IntelUtil.get_from_HDFS.get_filled_DF(ss, "20160701", "20160701")//.persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
//    var Data_0801 = IntelUtil.get_from_HDFS.get_filled_DF(ss, "20160801", "20160801")//.persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
//    var Data_0901 = IntelUtil.get_from_HDFS.get_filled_DF(ss, "20160901", "20160901")//.persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
//    var Data_1001 = IntelUtil.get_from_HDFS.get_filled_DF(ss, "20161001", "20161001")//.persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
//    var Data_1101 = IntelUtil.get_from_HDFS.get_filled_DF(ss, "20161101", "20161101")//.persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
 
   // var AllData = Data_0701.unionAll(Data_0801).unionAll(Data_0901).unionAll(Data_1001).unionAll(Data_1101).repartition(15000).persist(StorageLevel.MEMORY_AND_DISK_SER)
    var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, "20160701", "20160701")//.persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
  
    
    println("AllData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
     
    val pipeline_index = new Pipeline()
    
    //filled
    //pipeline_index.setStages(IntelUtil.funUtil.getPipeline(IntelUtil.constUtil.DisperseArr).toArray)
    
    
    //原始变量
    pipeline_index.setStages(IntelUtil.funUtil.idx_Pipeline(IntelUtil.constUtil.DisperseArr).toArray)


    println("start fitting data!")
    val index_Model = pipeline_index.fit(AllData)
      
    index_Model.save(idx_modelname)
    AllData.unpersist()
       
    println("my_index_Model saved in modelname in: " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
 
    
//    println("start transform data!")
//    val my_index_Model = PipelineModel.load(idx_modelname)
//    var vec_data = my_index_Model.transform(AllData)
//    println("Indexed done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
//    vec_data.show(10)
  
  }
  
    
}