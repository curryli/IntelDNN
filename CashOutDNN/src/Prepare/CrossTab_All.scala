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
//import org.apache.spark.mllib.stat.Statistics

import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.feature.ChiSqSelectorModel

object CrossTab_All {
    val startdate = IntelUtil.varUtil.startdate
    val enddate = IntelUtil.varUtil.enddate
    val usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
    val fraudType = "62"
        
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
     
    var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate)//.sample(false, 0.0000001, 0).cache
    println(AllData.count())
    //AllData.show()
    
    var fraud_join_Data = IntelUtil.get_from_HDFS.get_fraud_join_DF(ss, startdate, enddate).persist(StorageLevel.MEMORY_AND_DISK_SER)    
     

    println("fraudType_filled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
        
    //var normaldata_filled =AllData.except(fraudType_filled)
    
    //val fraud_key = fraudType_filled.select(concat($"sys_tra_no_filled", lit(" "), $"pri_acct_no_conv_filled", lit(" "), $"mchnt_cd_filled", lit(" "),$"pdate_filled"))
    
    val fraud_concat = fraud_join_Data.withColumn("concat_key", concat($"sys_tra_no", lit(" "), $"pri_acct_no_conv", lit(" "), $"mchnt_cd", lit(" "),$"pdate"))
    AllData = AllData.withColumn("concat_key", concat($"sys_tra_no", lit(" "), $"pri_acct_no_conv", lit(" "), $"mchnt_cd", lit(" "),$"pdate"))
   
    var fraudType_dir = rangedir + "fraudType_filled"
    var fraudType_filled = IntelUtil.get_from_HDFS.get_processed_DF(ss, fraudType_dir)
    
    var All_fraud = AllData.join(fraud_concat, AllData("concat_key")===fraud_concat("concat_key"), "leftouter").drop(AllData("concat_key"))
    //All_fraud.show(20)
    
    var normaldata_filled = All_fraud.filter(All_fraud("concat_key").isNull)     
    
    normaldata_filled = normaldata_filled.selectExpr(usedArr_filled:_*)                                           
    
    println("normaldata_filled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
           
    val udf_Map0 = udf[Double, String]{xstr => 0.0}
    val udf_Map1 = udf[Double, String]{xstr => 1.0}
         
    var NormalData_labeled = normaldata_filled.withColumn("label", udf_Map0(normaldata_filled("trans_md_filled")))
    var fraudType_labeled = fraudType_filled.withColumn("label", udf_Map1(fraudType_filled("trans_md_filled")))
    var labeledData = fraudType_labeled.unionAll(NormalData_labeled)
     
    val Arr_dist = labeledData.columns.toList.drop(4).dropRight(1).toArray   ///.dropRight(1)
  
    labeledData.describe().show
    
    for(col <- Arr_dist){
      labeledData.stat.crosstab(col, "label").show
      //println(labeledData.stat.corr(col, "label"))
    }
    
    //labeledData.stat.freqItems(Arr_dist, 0.5).show()
    //println(labeledData.stat.approxQuantile("trans_at", Array(0.2,0.4,0.6,0.8), 0.2))
    
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  

    
}