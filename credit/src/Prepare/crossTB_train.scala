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



object crossTB_train {

   
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
 
    
//    var Trans_ori_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/agg_cat.csv") 
//    //var Trans_ori_df = IntelUtil.get_from_HDFS.get_agg_DF(ss, "xrli/credit/agg_math.csv").persist(StorageLevel.MEMORY_AND_DISK_SER)  
//    
//     var label_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/train_label_encrypt.csv")
//    
//    label_df = label_df.select(label_df("certid").as("certid_label"), label_df("label"))
//    
//    Trans_ori_df = Trans_ori_df.join(label_df, Trans_ori_df("certid")===label_df("certid_label"), "left_outer").drop("certid_label")
//    
//    Trans_ori_df = Trans_ori_df.na.fill(-1)
//   
//   
//       
//    
//    val Arr_dist = Array("card_attr_cd_filled_idx-countDistinct","card_attr_cd_filled_idx-most_frequent_item","card_attr_cd_filled_idx-most_frequent_cnt","card_attr_cd_filled_idx-min","card_attr_cd_filled_idx-max","card_attr_cd_filled_idx-sum","card_attr_cd_filled_idx-median","card_attr_cd_filled_idx-peak_to_peak","is_risk_term_cd_filled_idx-sum","is_risk_cdhd_curr_cd_filled_idx-sum","is_risk_fwd_settle_cruu_cd_filled_idx-sum","trans_id_cd_filled_idx-countDistinct","trans_id_cd_filled_idx-most_frequent_item","trans_id_cd_filled_idx-most_frequent_cnt","trans_id_cd_filled_idx-min","trans_id_cd_filled_idx-max","trans_id_cd_filled_idx-sum","trans_id_cd_filled_idx-median","trans_id_cd_filled_idx-peak_to_peak","is_risk_pos_cond_cd_filled_idx-sum","trans_st_filled_idx-countDistinct","trans_st_filled_idx-most_frequent_item","trans_st_filled_idx-most_frequent_cnt","trans_st_filled_idx-min","trans_st_filled_idx-max","trans_st_filled_idx-sum","trans_st_filled_idx-median","trans_st_filled_idx-peak_to_peak","is_risk_orig_trans_st_filled_idx-sum","is_risk_cdhd_conv_rt_filled_idx-sum","term_cd_filled_idx-countDistinct","term_cd_filled_idx-most_frequent_item","term_cd_filled_idx-most_frequent_cnt","is_risk_card_accprt_nm_loc_filled_idx-sum","fwd_settle_cruu_cd_filled_idx-countDistinct","fwd_settle_cruu_cd_filled_idx-most_frequent_item","fwd_settle_cruu_cd_filled_idx-most_frequent_cnt","fwd_settle_cruu_cd_filled_idx-min","fwd_settle_cruu_cd_filled_idx-max","fwd_settle_cruu_cd_filled_idx-sum","fwd_settle_cruu_cd_filled_idx-median","fwd_settle_cruu_cd_filled_idx-peak_to_peak","pos_entry_md_cd_filled_idx-countDistinct","pos_entry_md_cd_filled_idx-most_frequent_item","pos_entry_md_cd_filled_idx-most_frequent_cnt","pos_entry_md_cd_filled_idx-min","pos_entry_md_cd_filled_idx-max","pos_entry_md_cd_filled_idx-sum","pos_entry_md_cd_filled_idx-median","pos_entry_md_cd_filled_idx-peak_to_peak","card_accprt_nm_loc_filled_idx-countDistinct","card_accprt_nm_loc_filled_idx-most_frequent_item","card_accprt_nm_loc_filled_idx-most_frequent_cnt","trans_curr_cd_filled_idx-countDistinct","trans_curr_cd_filled_idx-most_frequent_item","trans_curr_cd_filled_idx-most_frequent_cnt","trans_curr_cd_filled_idx-min","trans_curr_cd_filled_idx-max","trans_curr_cd_filled_idx-sum","trans_curr_cd_filled_idx-median","trans_curr_cd_filled_idx-peak_to_peak","is_risk_trans_st_filled_idx-sum","is_risk_mchnt_cd_filled_idx-sum","rcv_settle_curr_cd_filled_idx-countDistinct","rcv_settle_curr_cd_filled_idx-most_frequent_item","rcv_settle_curr_cd_filled_idx-most_frequent_cnt","rcv_settle_curr_cd_filled_idx-min","rcv_settle_curr_cd_filled_idx-max","rcv_settle_curr_cd_filled_idx-sum","rcv_settle_curr_cd_filled_idx-median","rcv_settle_curr_cd_filled_idx-peak_to_peak","is_risk_auth_id_resp_cd_filled_idx-sum","is_risk_fwd_settle_conv_rt_filled_idx-sum","card_no-countDistinct","is_risk_mcc_cd_filled_idx-sum","cdhd_conv_rt_filled_idx-countDistinct","cdhd_conv_rt_filled_idx-most_frequent_item","cdhd_conv_rt_filled_idx-most_frequent_cnt","cdhd_conv_rt_filled_idx-min","cdhd_conv_rt_filled_idx-max","cdhd_conv_rt_filled_idx-sum","cdhd_conv_rt_filled_idx-median","cdhd_conv_rt_filled_idx-peak_to_peak","trans_chnl_filled_idx-countDistinct","trans_chnl_filled_idx-most_frequent_item","trans_chnl_filled_idx-most_frequent_cnt","trans_chnl_filled_idx-min","trans_chnl_filled_idx-max","trans_chnl_filled_idx-sum","trans_chnl_filled_idx-median","trans_chnl_filled_idx-peak_to_peak","is_risk_iss_ins_cd_filled_idx-sum","card_media_cd_filled_idx-countDistinct","card_media_cd_filled_idx-most_frequent_item","card_media_cd_filled_idx-most_frequent_cnt","card_media_cd_filled_idx-min","card_media_cd_filled_idx-max","card_media_cd_filled_idx-sum","card_media_cd_filled_idx-median","card_media_cd_filled_idx-peak_to_peak","is_risk_trans_curr_cd_filled_idx-sum","is_risk_card_media_cd_filled_idx-sum","is_risk_rcv_settle_conv_rt_filled_idx-sum","is_risk_trans_id_cd_filled_idx-sum","mchnt_cd_filled_idx-countDistinct","mchnt_cd_filled_idx-most_frequent_item","mchnt_cd_filled_idx-most_frequent_cnt","pos_cond_cd_filled_idx-countDistinct","pos_cond_cd_filled_idx-most_frequent_item","pos_cond_cd_filled_idx-most_frequent_cnt","pos_cond_cd_filled_idx-min","pos_cond_cd_filled_idx-max","pos_cond_cd_filled_idx-sum","pos_cond_cd_filled_idx-median","pos_cond_cd_filled_idx-peak_to_peak","fwd_settle_conv_rt_filled_idx-countDistinct","fwd_settle_conv_rt_filled_idx-most_frequent_item","fwd_settle_conv_rt_filled_idx-most_frequent_cnt","fwd_settle_conv_rt_filled_idx-min","fwd_settle_conv_rt_filled_idx-max","fwd_settle_conv_rt_filled_idx-sum","fwd_settle_conv_rt_filled_idx-median","fwd_settle_conv_rt_filled_idx-peak_to_peak","auth_id_resp_cd_filled_idx-countDistinct","auth_id_resp_cd_filled_idx-most_frequent_item","auth_id_resp_cd_filled_idx-most_frequent_cnt","resp_cd_filled_idx-countDistinct","resp_cd_filled_idx-most_frequent_item","resp_cd_filled_idx-most_frequent_cnt","resp_cd_filled_idx-min","resp_cd_filled_idx-max","resp_cd_filled_idx-sum","resp_cd_filled_idx-median","resp_cd_filled_idx-peak_to_peak","mcc_cd_filled_idx-countDistinct","mcc_cd_filled_idx-most_frequent_item","mcc_cd_filled_idx-most_frequent_cnt","mcc_cd_filled_idx-min","mcc_cd_filled_idx-max","mcc_cd_filled_idx-sum","mcc_cd_filled_idx-median","mcc_cd_filled_idx-peak_to_peak","orig_trans_st_filled_idx-countDistinct","orig_trans_st_filled_idx-most_frequent_item","orig_trans_st_filled_idx-most_frequent_cnt","orig_trans_st_filled_idx-min","orig_trans_st_filled_idx-max","orig_trans_st_filled_idx-sum","orig_trans_st_filled_idx-median","orig_trans_st_filled_idx-peak_to_peak","iss_ins_cd_filled_idx-countDistinct","iss_ins_cd_filled_idx-most_frequent_item","iss_ins_cd_filled_idx-most_frequent_cnt","iss_ins_cd_filled_idx-min","iss_ins_cd_filled_idx-max","iss_ins_cd_filled_idx-sum","iss_ins_cd_filled_idx-median","iss_ins_cd_filled_idx-peak_to_peak","rcv_settle_conv_rt_filled_idx-countDistinct","rcv_settle_conv_rt_filled_idx-most_frequent_item","rcv_settle_conv_rt_filled_idx-most_frequent_cnt","rcv_settle_conv_rt_filled_idx-min","rcv_settle_conv_rt_filled_idx-max","rcv_settle_conv_rt_filled_idx-sum","rcv_settle_conv_rt_filled_idx-median","rcv_settle_conv_rt_filled_idx-peak_to_peak","is_risk_resp_cd_filled_idx-sum","is_risk_card_attr_cd_filled_idx-sum","is_risk_rcv_settle_curr_cd_filled_idx-sum","is_risk_pos_entry_md_cd_filled_idx-sum","cdhd_curr_cd_filled_idx-countDistinct","cdhd_curr_cd_filled_idx-most_frequent_item","cdhd_curr_cd_filled_idx-most_frequent_cnt","cdhd_curr_cd_filled_idx-min","cdhd_curr_cd_filled_idx-max","cdhd_curr_cd_filled_idx-sum","cdhd_curr_cd_filled_idx-median","cdhd_curr_cd_filled_idx-peak_to_peak","is_risk_trans_chnl_filled_idx-sum")
//    Trans_ori_df.describe().show
//    
//    for(col <- Arr_dist){
//        var tempdf = Trans_ori_df.stat.crosstab(col, "label")
//        tempdf = tempdf.withColumn("ratio", tempdf("1")/tempdf("0"))
//        tempdf = tempdf.coalesce(1)
//        var savepath = "xrli/credit/train_cross/" + col + ".csv"
//        val saveOptions = Map("header" -> "true", "path" -> savepath)
//        tempdf.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    
    var Trans_ori_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/agg_math_cross.csv") 
    //var Trans_ori_df = IntelUtil.get_from_HDFS.get_agg_DF(ss, "xrli/credit/agg_math.csv").persist(StorageLevel.MEMORY_AND_DISK_SER)  
    
     var label_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/train_label_encrypt.csv")
    
    label_df = label_df.select(label_df("certid").as("certid_label"), label_df("label").as("true_label"))
    
    Trans_ori_df = Trans_ori_df.join(label_df, Trans_ori_df("certid")===label_df("certid_label"), "left_outer").drop("certid_label")
    
    Trans_ori_df = Trans_ori_df.na.fill(-1)
   
   
       
    
    val Arr_dist = Array("month_No-cnt_0","month_No-cnt_1","month_No-cnt_2","month_No-cnt_3","month_No-cnt_4","month_No-cnt_5","month_No-cnt_6","month_No-cnt_7","month_No-cnt_8","month_No-cnt_9","month_No-cnt_10","month_No-cnt_11","month_No-cnt_12","month_No-cnt_13","month_No-cnt_14","month_No-cnt_15","month_No-cnt_16","month_No-cnt_17","month_No-cnt_18","month_No-cnt_19","month_No-cnt_20","month-countDistinct","month-most_frequent_item","month-most_frequent_cnt","month-median","month-peak_to_peak","weekday-countDistinct","weekday-most_frequent_item","weekday-most_frequent_cnt","weekday-min","weekday-max","weekday-sum","weekday-median","month_cnt_0","month_cnt_1","month_cnt_2","month_cnt_3","month_cnt_4","month_cnt_5","month_cnt_6","month_cnt_7","month_cnt_8","month_cnt_9","month_cnt_10","month_cnt_11","month_cnt_12","month_cnt_13","month_cnt_14","month_cnt_15","month_cnt_16","month_cnt_17","month_cnt_18","month_cnt_19","apply_max_delta","min_apply_delta","label","has_trans_month","trans_month_max")
    
    
    Trans_ori_df.describe().show
    
    for(col <- Arr_dist){
        var tempdf = Trans_ori_df.stat.crosstab(col, "label")
        tempdf = tempdf.withColumn("ratio", tempdf("1")/tempdf("0"))
        tempdf = tempdf.coalesce(1)
        var savepath = "xrli/credit/train_cross_math/" + col + ".csv"
        val saveOptions = Map("header" -> "true", "path" -> savepath)
        tempdf.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
        
   
        
      println(Trans_ori_df.stat.corr(col, "label"))
    }
    
    
    
    
    
    
    
    
    
    
//    var Trans_ori_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/agg_math.csv") 
//    //var Trans_ori_df = IntelUtil.get_from_HDFS.get_agg_DF(ss, "xrli/credit/agg_math.csv").persist(StorageLevel.MEMORY_AND_DISK_SER)  
//    
//     
//    val Arr_dist = Array("apply_dateNo","sex_female","sex_male","age","hour-countDistinct","hour-most_frequent_item","hour-most_frequent_cnt","hour-min","hour-peak_to_peak","dateNo-countDistinct","dateNo-most_frequent_item","dateNo-most_frequent_cnt","dateNo-min","dateNo-max","month-most_frequent_item","fund_shortage-sum","weekday-countDistinct","weekday-most_frequent_item","weekday-most_frequent_cnt","weekday-peak_to_peak","Trans_at-count","date-countDistinct","date-most_frequent_item","date-most_frequent_cnt","date-min","card_no-countDistinct","stageInMonth-countDistinct","stageInMonth-most_frequent_item","stageInMonth-most_frequent_cnt","stageInMonth-min","stageInMonth-max","stageInMonth-median","stageInMonth-peak_to_peak","min_apply_delta","aera_code_encode","is_risk_aera_code","prov","city","county","age_section")
//    Trans_ori_df.describe().show
//    
//    for(col <- Arr_dist){
//        var tempdf = Trans_ori_df.stat.crosstab(col, "label")
//        tempdf.show
//        tempdf = tempdf.coalesce(1)
//        var savepath = "xrli/credit/train_cross/" + col + ".csv"
//        val saveOptions = Map("header" -> "true", "path" -> savepath)
//        tempdf.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
//   
//        
//      println(Trans_ori_df.stat.corr(col, "label"))
//    }
    
  
    
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  

    
}