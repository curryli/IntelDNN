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


object Save_N_F_split {
    val startdate = "20161001"            //IntelUtil.varUtil.startdate
    val enddate = "20161231"                        //IntelUtil.varUtil.enddate
    val rangedir = IntelUtil.varUtil.testDir                                //IntelUtil.varUtil.rangeDir 
    val usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
    
    //可以调整
    val sample_cards_ratio = 0.0005
    val TF_ratio = 1000
    val fraudType = "04"
       
    var fraudType_cards_num = 0L
    var normal_cards_num = 0L
    var fraudType_related_fraud_count = 0L
    
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
      .config("spark.driver.maxResultSize", "10g")
      .config("spark.debug.maxToStringFields",500)
      .getOrCreate()
  
    import ss.implicits._
    import ss.sql
 
    val startTime = System.currentTimeMillis(); 
 
    Save_split(ss)
    println("step4 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
 
  
  }
    
    
     
   def drop_confuse_6(ss: SparkSession ): Unit ={
     val sc = ss.sparkContext
     var FE_Data = IntelUtil.get_from_HDFS.get_FE_DF(ss, rangedir + "FE_db").persist(StorageLevel.MEMORY_AND_DISK_SER)
     
     val label= "label" 
     val cardcol= "pri_acct_no_conv"
    
     var Fraud = FE_Data.filter(FE_Data("label")===1)
     val card_c_F = Fraud.select("pri_acct_no_conv").distinct().rdd.map(x=>x.getString(0)).collect()
//     真实比例150000
//     [[29778984     2161]
//     [   12805     4158]]
     
     Fraud = Fraud.sample(false, 0.012)
      
     val Normal = FE_Data.filter(FE_Data("label")===0)
    
     println("Normal count: ", Normal.count())
     
     
 
     val fine_N = Normal.filter(!Normal("pri_acct_no_conv").isin(card_c_F:_*))
     println("fine_N: ", fine_N.count())
    
     val last_data = Fraud.union(fine_N)
     last_data.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "FE_test_real") 
     println("Fraud count: ", Fraud.count(), "fine_N  count: ", fine_N.count())
    
//     (Normal count: ,29841749)
//(fine_N: ,29781145)
//(Fraud count: ,215,fine_N  count: ,29781145)

    }
  
  
   def Save_split(ss: SparkSession ): Unit ={
     val sc = ss.sparkContext
     var FE_Data = IntelUtil.get_from_HDFS.get_FE_DF(ss, rangedir + "FE_db").persist(StorageLevel.MEMORY_AND_DISK_SER)
     
     val label= "label" 
     val cardcol= "pri_acct_no_conv"
    
     var Fraud = FE_Data.filter(FE_Data("label")===1)
      
     val countchnl_F = Fraud.groupBy("card_attr_idx").agg(count("trans_at") as "counts") 
     countchnl_F.show()
     
     val card_c_F = Fraud.select("pri_acct_no_conv").distinct().rdd.map(x=>x.getString(0)).collect()
     
     val Normal = FE_Data.filter(FE_Data("label")===0)
     println("Normal count: ", Normal.count())
     val fine_N = Normal.filter(!Normal("pri_acct_no_conv").isin(card_c_F:_*))
     println("fine_N: ", fine_N.count())
     
     val countchnl_N = fine_N.groupBy("card_attr_idx").agg(count("trans_at") as "counts") 
     countchnl_N.show()
     
//     真实比例150000
//     [[29778984     2161]
//     [   12805     4158]]
     
    
     var Array(trainNormal, testNormal) = fine_N.randomSplit(Array(0.03, 0.97))
     println("trainNormal: ", trainNormal.count(), "testNormal: ", testNormal.count())
     
     trainNormal.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "FE_trainNormal") 
     testNormal.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "FE_testNormal")
     
     Fraud.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "FE_Fraud")
    
//     (fine_N: ,29781145)
//(trainFraud: ,16607,testFraud: ,355)
//(trainNormal: ,1489925,testNormal: ,28291220)
    }
   
   
   
//   --------------+------+
//|trans_chnl_idx|counts|
//+--------------+------+
//|           1.0|   778|
//|           0.0| 14796|
//|           9.0|    21|
//|          10.0|     2|
//|           5.0|     4|
//|           4.0|   406|
//|           2.0|   900|
//|           3.0|    55|
//+--------------+------+
//
//+--------------+--------+
//|trans_chnl_idx|  counts|
//+--------------+--------+
//|           1.0| 3061108|
//|          15.0|     375|
//|          17.0|     150|
//|           0.0|24802119|
//|           9.0|   39064|
//|          12.0|    5618|
//|          10.0|   33982|
//|          14.0|    5336|
//|          19.0|      15|
//|           5.0|   77658|
//|           6.0|    6393|
//|           4.0|  321389|
//|           2.0| 1149426|
//|           8.0|   10478|
//|           3.0|  337891|
//|          16.0|     571|
//|          13.0|    7138|
//+--------------+--------+
//   
   
}