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


object counterfeit_by_Account {
  

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
  
    val AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000).persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    AllData.show(5)
    println("AllData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
       
    var fraud_join_Data = IntelUtil.get_from_HDFS.get_fraud_join_DF(ss, startdate, enddate).repartition(100).cache 
    println("fraud_join_Data count is " + fraud_join_Data.count())   
    fraud_join_Data.show(5)
     
    //sys_tra_no, pri_acct_no_conv, mchnt_cd, pdate 
    val All_fraud = AllData.join(fraud_join_Data, AllData("sys_tra_no")===fraud_join_Data("sys_tra_no") 
                                               && AllData("pri_acct_no_conv")===fraud_join_Data("pri_acct_no_conv")
                                               && AllData("mchnt_cd")===fraud_join_Data("mchnt_cd")
                                               && AllData("pdate")===fraud_join_Data("pdate"), "leftsemi").sample(false, 0.0001, 0)
 
    println("AllData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
 
   
    val account_sample = AllData.select("pri_acct_no_conv").sample(false, 0.000002, 0).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val account_fraud = All_fraud.select("pri_acct_no_conv")
    val account_sample_normal = account_sample.except(account_fraud)
    println("account_sample_normal count is " + account_sample_normal.count())
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////    
    
    
    var Normal_sample = AllData.join(account_sample_normal, AllData("pri_acct_no_conv")===account_sample_normal("pri_acct_no_conv"), "left_semi").persist(StorageLevel.MEMORY_AND_DISK_SER)
    var Normal_sample_filled = Normal_sample.selectExpr(usedArr_filled:_*).persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    println("Normal_sample_filled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
     println("Normal_sample_filled count is " + Normal_sample_filled.count())
     
    var counterfeit_infraud = fraud_join_Data.filter(fraud_join_Data("fraud_tp")==="04") 
    println("counterfeit count is " + counterfeit_infraud.count())
    fraud_join_Data.unpersist(false)
    
    var counterfeit_inAll = AllData.join(counterfeit_infraud, AllData("sys_tra_no")===counterfeit_infraud("sys_tra_no") 
                                               && AllData("pri_acct_no_conv")===counterfeit_infraud("pri_acct_no_conv")
                                               && AllData("mchnt_cd")===counterfeit_infraud("mchnt_cd")
                                               && AllData("pdate")===counterfeit_infraud("pdate"), "leftsemi").persist(StorageLevel.MEMORY_AND_DISK_SER) 
                                               
  
    ////////////////////////////////FraudData
    val counterfeit_filled = counterfeit_inAll.selectExpr(usedArr_filled:_*)
    println("counterfeit_filled count is " + counterfeit_filled.count())
   
    val AllData_filled = AllData.selectExpr(usedArr_filled:_*)
/////////////////////////////////////////////////////////////    
    var fraud_related_Data_filled = AllData_filled.join(counterfeit_filled, AllData_filled("pri_acct_no_conv_filled")===counterfeit_filled("pri_acct_no_conv_filled"), "left_semi") 
    
    val fraud_related_Data_Normal_filled = fraud_related_Data_filled.except(counterfeit_filled)
    println("fraud_related_Data_Normal_filled count is " + fraud_related_Data_Normal_filled.count())
     
     ////////////////////////////////NormalData
    Normal_sample_filled = Normal_sample_filled.unionAll(fraud_related_Data_Normal_filled)
//////////////////////////////////////////////////////////////////////////////////////
    //AllData.unpersist(false)
    
    // Column isFraud (label) must be of type DoubleType
    val udf_Map0 = udf[Double, String]{xstr => 0.0}
    val udf_Map1 = udf[Double, String]{xstr => 1.0}
    
    var NormalData_filled = Normal_sample_filled.withColumn("isFraud", udf_Map0(Normal_sample_filled("trans_md_filled")))
    var FraudData_filled = counterfeit_filled.withColumn("isFraud", udf_Map1(counterfeit_filled("trans_md_filled")))
    var LabeledData_filled = NormalData_filled.unionAll(FraudData_filled)
	  Normal_sample_filled.unpersist(false)
	
    println("LabeledData_filled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    LabeledData_filled.show(5)
    
    //LabeledData_filled.repartition(10).rdd.map(_.mkString(",")).saveAsTextFile("xrli/IntelDNN/LabeledData_byACccount")
    
    AllData.unpersist(false)
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
  }
  
    
}