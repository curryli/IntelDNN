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
import scala.reflect.ClassTag
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.feature.ChiSqSelectorModel



object mchant_analysis {

  
   def any_to_double[T: ClassTag](b: T):Double={
    if(b==true)
      1.0
    else
      0
  }

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
      .getOrCreate()
  
    import ss.implicits._
    import ss.sql
 
    val startTime = System.currentTimeMillis(); 
 
    val train_ori_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/train_trans_encrypt.csv")
    val test_ori_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/test_trans_encrypt.csv")
     
    var Trans_ori_df =  train_ori_df.unionAll(test_ori_df)
  
       var label_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/train_label_encrypt.csv")
    
    label_df = label_df.select(label_df("certid").as("certid_label"), label_df("label"))
    
   Trans_ori_df =Trans_ori_df.join(label_df, Trans_ori_df("certid")===label_df("certid_label"), "left_outer").drop("certid_label")
    
    
    Trans_ori_df = Trans_ori_df.na.fill(-1)
   
        
     
    var Good_df = Trans_ori_df.filter(Trans_ori_df("label")=== 1)
    var Bad_df = Trans_ori_df.filter(Trans_ori_df("label")=== 0)
    var Bad_items = Bad_df.select("card_accprt_nm_loc").distinct()
    var Good_items = Good_df.select("card_accprt_nm_loc").distinct()
    var risk_items =  Bad_items.except(Good_items) 
    
    var mchnt_Bad_list = Bad_items.map(x=>x.getString(0)).collect()
    var mchnt_risk_list = risk_items.map(x=>x.getString(0)).collect()
    
//    mchnt_Bad_list.foreach {println}
//    mchnt_risk_list.foreach {println}
//    
    
    val mchnt_Bad = udf[Double, String]{xstr => any_to_double(mchnt_Bad_list.contains(xstr))}    
    Trans_ori_df = Trans_ori_df.withColumn("is_mchnt_Bad",mchnt_Bad(Trans_ori_df("card_accprt_nm_loc")))
    var count_mcc_df = Trans_ori_df.groupBy("certid").agg(sum("is_mchnt_Bad") as "mchnt_Bad_cnt") 
   
    val mchnt_risk = udf[Double, String]{xstr => any_to_double(mchnt_risk_list.contains(xstr))}    
    Trans_ori_df = Trans_ori_df.withColumn("is_mchnt_risk",mchnt_risk(Trans_ori_df("card_accprt_nm_loc")))
    var tmpdf = Trans_ori_df.groupBy("certid").agg(sum("is_mchnt_risk") as "mchnt_risk_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("mchnt_risk_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    
    
    
    //////////////////////////
     
    Bad_items = Bad_df.select("mchnt_cd").distinct()
    Good_items = Good_df.select("mchnt_cd").distinct()
    risk_items = Bad_items.except(Good_items) 
    
    val mchntcd_Bad_list = Bad_items.map(x=>x.getString(0)).collect()
    val mchntcd_risk_list = risk_items.map(x=>x.getString(0)).collect()
    
//    mchntcd_Bad_list.foreach {println}
//    mchntcd_risk_list.foreach {println}
    
    val mchntcd_Bad = udf[Double, String]{xstr => any_to_double(mchntcd_Bad_list.contains(xstr))}    
    Trans_ori_df = Trans_ori_df.withColumn("is_mchntcd_Bad",mchntcd_Bad(Trans_ori_df("mchnt_cd")))
    tmpdf = Trans_ori_df.groupBy("certid").agg(sum("is_mchntcd_Bad") as "mchntcd_Bad_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("mchntcd_Bad_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val mchntcd_risk = udf[Double, String]{xstr => any_to_double(mchntcd_risk_list.contains(xstr))}    
    Trans_ori_df = Trans_ori_df.withColumn("is_mchntcd_risk",mchntcd_risk(Trans_ori_df("mchnt_cd")))
    tmpdf = Trans_ori_df.groupBy("certid").agg(sum("is_mchntcd_risk") as "mchntcd_risk_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("mchntcd_risk_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
     
    
   
   
   val has_mchnt_Bad = udf[Double, Int]{xstr => any_to_double(xstr>0)}
   count_mcc_df = count_mcc_df.withColumn("has_mchnt_Bad", has_mchnt_Bad(count_mcc_df("mchnt_Bad_cnt")))
   
   val has_mchnt_risk = udf[Double, Int]{xstr => any_to_double(xstr>0)}
   count_mcc_df = count_mcc_df.withColumn("has_mchnt_risk", has_mchnt_risk(count_mcc_df("mchnt_risk_cnt")))
   
   val has_mchntcd_Bad = udf[Double, Int]{xstr => any_to_double(xstr>0)}
   count_mcc_df = count_mcc_df.withColumn("has_mchntcd_Bad", has_mchntcd_Bad(count_mcc_df("mchntcd_Bad_cnt")))
   
   val has_mchntcd_risk = udf[Double, Int]{xstr => any_to_double(xstr>0)}
   count_mcc_df = count_mcc_df.withColumn("has_mchntcd_risk", has_mchntcd_risk(count_mcc_df("mchntcd_risk_cnt")))
    
   
   
   tmpdf = Trans_ori_df.groupBy("certid").agg(count("certid") as "trans_cnt") 
   tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("trans_cnt"))
   count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
   
   
   count_mcc_df = count_mcc_df.withColumn("mchntcd_Bad_ratio", count_mcc_df("mchnt_Bad_cnt")/count_mcc_df("trans_cnt") )
   count_mcc_df = count_mcc_df.withColumn("mchnt_risk_ratio", count_mcc_df("mchnt_risk_cnt")/count_mcc_df("trans_cnt") )
   count_mcc_df = count_mcc_df.withColumn("mchntcd_Bad_ratio", count_mcc_df("mchntcd_Bad_cnt")/count_mcc_df("trans_cnt") )
   count_mcc_df = count_mcc_df.withColumn("mchntcd_risk_ratio", count_mcc_df("mchntcd_risk_cnt")/count_mcc_df("trans_cnt") )
   
   count_mcc_df = count_mcc_df.join(label_df, count_mcc_df("certid")===label_df("certid_label"), "left_outer").drop("certid_label")
   
   
   
   count_mcc_df = count_mcc_df.coalesce(1)
   var savepath = "xrli/credit/mchnt_ana_right.csv"
   val saveOptions = Map("header" -> "true", "path" -> savepath)
   count_mcc_df.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
    
    
    
    
    
     
  }
  
  

    
}