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



object check {

  
   val catgory_many_list = Array("mchnt_cd", "card_accprt_nm_loc","term_cd","auth_id_resp_cd")
   
   val catgory_little_list = Array("iss_ins_cd", "trans_chnl", "mcc_cd", "resp_cd", "trans_id_cd", "orig_trans_st","trans_st", "trans_curr_cd",
                "fwd_settle_cruu_cd", "fwd_settle_conv_rt", "rcv_settle_curr_cd","rcv_settle_conv_rt", "cdhd_curr_cd",
                "cdhd_conv_rt", "card_attr_cd","card_media_cd", "pos_cond_cd", "pos_entry_md_cd")
                
   val catgory_list = catgory_many_list.++:(catgory_little_list)
   
   val not_train_list = Array("retri_ref_no", "Sys_tra_no", "Trans_tm","Settle_dt","fwd_settle_at","rcv_settle_at","cdhd_at","certid","card_no","label","Trans_at")

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
 
    var train_ori_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/train_trans_encrypt.csv")
    var test_ori_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/test_trans_encrypt.csv")
    
    train_ori_df = train_ori_df.drop("card_accprt_nm_loc")
    train_ori_df.rdd.map(_.mkString(",")).saveAsTextFile("xrli/credit/train_ori_df")
    
    test_ori_df = test_ori_df.drop("card_accprt_nm_loc")
    test_ori_df.rdd.map(_.mkString(",")).saveAsTextFile("xrli/credit/test_ori_df")
    
    
    
     
//    var Trans_ori_df =  train_ori_df.unionAll(test_ori_df)
//    
//    Trans_ori_df = Trans_ori_df.na.fill(-1)
    
//    var checklist_train = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/checklst_train.csv")
//    checklist_train = checklist_train.select(checklist_train("certid").as("certid_label"))
//    Trans_ori_df = Trans_ori_df.join(checklist_train, Trans_ori_df("certid")===checklist_train("certid_label"), "right_outer").drop("certid_label")
//    Trans_ori_df = Trans_ori_df.coalesce(1)
//    var savepath = "xrli/credit/checked_train.csv"
//    val saveOptions = Map("header" -> "true", "path" -> savepath)
//    Trans_ori_df.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
//    
//    var checklist_test = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/checklst_test.csv")
//    checklist_test = checklist_test.select(checklist_test("certid").as("certid_label"))
//    Trans_ori_df = Trans_ori_df.join(checklist_test, Trans_ori_df("certid")===checklist_test("certid_label"), "right_outer").drop("certid_label")
//    Trans_ori_df = Trans_ori_df.coalesce(1)
//    var savepath_2 = "xrli/credit/checked_test.csv"
//    val saveOptions_2 = Map("header" -> "true", "path" -> savepath_2)
//    Trans_ori_df.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions_2).save()
    
    
     
  }
  
  

    
}