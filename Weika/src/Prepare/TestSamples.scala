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
import org.apache.spark.ml.feature.QuantileDiscretizer
import scala.collection.mutable.HashMap


object TestSamples {
 
    val startdate = IntelUtil.varUtil.startdate
    val enddate = IntelUtil.varUtil.enddate
    val rangedir = IntelUtil.varUtil.testDir 
 
   
  def main(args: Array[String]): Unit = {

    //屏蔽日志
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.ERROR);
    Logger.getLogger("parse").setLevel(Level.ERROR); 
    
    //val sparkConf = new SparkConf().setAppName("spark2SQL")
    val warehouseLocation = "spark-warehouse"
    
    val ss = SparkSession
      .builder()
      .appName("Save_IndexerPipeLine")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.schema.verification", false)
      .config("spark.debug.maxToStringFields",500)
      .getOrCreate()
  
    import ss.implicits._
    import ss.sql
 
    val startTime = System.currentTimeMillis(); 
 
    var input_dir = rangedir + "Labeled_All"
    var labeledData = IntelUtil.get_from_HDFS.get_Labeled_All(ss, input_dir).persist(StorageLevel.MEMORY_AND_DISK_SER)
     
    
    
    var new_labeled = labeledData.sample(false, 0.00001, 0).persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("new_labeled count ", new_labeled.count())
    println("new_labeled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
        
    labeledData.unpersist(blocking=false)
     
    new_labeled = FE_new.FE_function(ss, new_labeled) 
    
    
    
    println("FE done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
    ////////////////////////////////////////
    var All_cols =  new_labeled.columns
    
    var Arr_to_idx = IntelUtil.constUtil.DisperseArr
    
    val no_idx_arr = All_cols.toSet.diff(Arr_to_idx.+:("pri_acct_no_conv").+:("label").toSet).toList
    

    val CatVecArr = Arr_to_idx.map { x => x + "_idx"}
    
    var idx_modelname = IntelUtil.varUtil.idx_model
    val my_index_Model = PipelineModel.load(idx_modelname)
   
    new_labeled = my_index_Model.transform(new_labeled)
      
    println("Index pipeline done." )
    
    val feature_arr = no_idx_arr.++(CatVecArr)      //加载idx_model时使用
    println("feature_arr: ", feature_arr.mkString(","))
    
    new_labeled = new_labeled.selectExpr(List("pri_acct_no_conv","label").++(feature_arr):_*) 
 
    new_labeled.show(5)
    
    println("start change to double")

    var db_list = List("pri_acct_no_conv","label")
    
     for(col <- feature_arr){
        val newcol = col + "_db" 
        db_list = db_list.:+(newcol)
        new_labeled = new_labeled.withColumn(newcol, new_labeled.col(col).cast(DoubleType))
     }
 
    println("change to double done.")
  
     
    new_labeled = new_labeled.na.fill(-1.0)   // 因为这里填的是-1.0，是double类型，所以  好像只能对double类型的列起作用。  如果填充“1”，那么只对String类型起作用。
   
    new_labeled = new_labeled.selectExpr(db_list:_*) 
    
      ////////////////////////////////////////  
    
    new_labeled = new_labeled.filter(new_labeled("label").===(1)) 
    println("new_labeled2 count ", new_labeled.count())
    //new_labeled.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "tmp")
    //println("new_labeled2 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
     
     
  }
  
  
  
    
}