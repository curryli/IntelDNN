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


object counterfeit_Cross_by_Account {
  

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
    var usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
  
    val AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000).cache//.persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    AllData.show(5)
    println("AllData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
    var All_Cross = AllData.filter(AllData("cross_dist_in")==="1").repartition(1000).persist(StorageLevel.MEMORY_AND_DISK_SER) 
    println("All_Cross count is " + All_Cross.count())  //8千万的2百万左右
  
    var fraud_join_Data = IntelUtil.get_from_HDFS.get_fraud_join_DF(ss, startdate, enddate).repartition(100).persist(StorageLevel.MEMORY_AND_DISK_SER) 
    println("fraud_join_Data count is " + fraud_join_Data.count())   
    fraud_join_Data.show(5)
     
    //sys_tra_no, pri_acct_no_conv, mchnt_cd, pdate 
    val All_fraud_Cross = All_Cross.join(fraud_join_Data, All_Cross("sys_tra_no")===fraud_join_Data("sys_tra_no") 
                                               && All_Cross("pri_acct_no_conv")===fraud_join_Data("pri_acct_no_conv")
                                               && All_Cross("mchnt_cd")===fraud_join_Data("mchnt_cd")
                                               && All_Cross("pdate")===fraud_join_Data("pdate"), "leftsemi").persist(StorageLevel.MEMORY_AND_DISK_SER) 
  
    println("All_fraud_Cross count is " + All_fraud_Cross.count())
    All_fraud_Cross.show(5)
    println("All_fraud_Cross done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
  
 
    val All_Cross_filled = All_Cross.sample(false, 0.00001, 0).selectExpr(usedArr_filled:_*)
    val All_fraud_Cross_filled = All_fraud_Cross.selectExpr(usedArr_filled:_*)
    
    ////////////////////////////////NormalData
    var Normal_Cross_filled = All_Cross_filled.except(All_fraud_Cross_filled).persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("Normal_Cross_filled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
     
    var counterfeit = fraud_join_Data.filter(fraud_join_Data("fraud_tp")==="04").persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("counterfeit count is " + counterfeit.count())
     
    //sys_tra_no, pri_acct_no_conv, mchnt_cd, pdate 
    var counterfeit_Cross = All_Cross.join(counterfeit, All_fraud_Cross("sys_tra_no")===counterfeit("sys_tra_no") 
                                               && All_fraud_Cross("pri_acct_no_conv")===counterfeit("pri_acct_no_conv")
                                               && All_fraud_Cross("mchnt_cd")===counterfeit("mchnt_cd")
                                               && All_fraud_Cross("pdate")===counterfeit("pdate"), "leftsemi").persist(StorageLevel.MEMORY_AND_DISK_SER) 
    All_fraud_Cross.unpersist(false)
    counterfeit.unpersist(false)
    println("counterfeit_Cross done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
       
    
    ////////////////////////////////FraudData
    val counterfeit_Cross_filled = counterfeit_Cross.selectExpr(usedArr_filled:_*)
    println("counterfeit_Cross_filled count is " + counterfeit_Cross_filled.count())
    println("Normal_Cross_filled count is " + Normal_Cross_filled.count())
 
/////////////////////////////////////////////////////////////    
    var fraud_related_Data = AllData.join(counterfeit, AllData("pri_acct_no_conv")===counterfeit("pri_acct_no_conv"), "left_semi") 
    var fraud_related_Data_filled = fraud_related_Data.selectExpr(usedArr_filled:_*)
    val fraud_related_Data_normal_filled = fraud_related_Data_filled.except(counterfeit_Cross_filled)
    println("fraud_related_Data_normal_filled count is " + fraud_related_Data_normal_filled.count())
     
    Normal_Cross_filled = Normal_Cross_filled.unionAll(fraud_related_Data_normal_filled)
//////////////////////////////////////////////////////////////////////////////////////
    All_Cross.unpersist(false)
    
    // Column isFraud (label) must be of type DoubleType
    val udf_Map0 = udf[Double, String]{xstr => 0.0}
    val udf_Map1 = udf[Double, String]{xstr => 1.0}
    
    var NormalData_filled = Normal_Cross_filled.withColumn("isFraud", udf_Map0(Normal_Cross_filled("trans_md_filled")))
    var FraudData_filled = counterfeit_Cross_filled.withColumn("isFraud", udf_Map1(counterfeit_Cross_filled("trans_md_filled")))
    var LabeledData_filled = NormalData_filled.unionAll(FraudData_filled)
    println("LabeledData_filled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    LabeledData_filled.show(5)
    LabeledData_filled.sort(LabeledData_filled("pri_acct_no_conv_filled")).repartition(1).rdd.map(_.mkString(",")).saveAsTextFile("xrli/IntelDNN/LabeledData_test")
    
    //AllData.unpersist(false)
   
  }
  
    
}