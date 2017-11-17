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



object join_HR_FE {

  
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
 
    var Trans_ori_df = IntelUtil.get_from_HDFS.get_FE_DF(ss, "xrli/credit/cert_all_right").persist(StorageLevel.MEMORY_AND_DISK_SER)  
    
     
    val Arr_dist = Array("iss_ins_cd_filled_idx","trans_chnl_filled_idx","mcc_cd_filled_idx","resp_cd_filled_idx","trans_id_cd_filled_idx","orig_trans_st_filled_idx","trans_st_filled_idx","trans_curr_cd_filled_idx","fwd_settle_cruu_cd_filled_idx","fwd_settle_conv_rt_filled_idx","rcv_settle_curr_cd_filled_idx","rcv_settle_conv_rt_filled_idx","cdhd_curr_cd_filled_idx","cdhd_conv_rt_filled_idx","card_attr_cd_filled_idx","card_media_cd_filled_idx","pos_cond_cd_filled_idx","pos_entry_md_cd_filled_idx")   ///.dropRight(1)
  
    val many_dist = Array("auth_id_resp_cd_filled_idx", "card_accprt_nm_loc_filled_idx", "mchnt_cd_filled_idx","term_cd_filled_idx")
    
    Trans_ori_df.describe().show
    
    for(col <- many_dist){
        var tempdf = Trans_ori_df.stat.crosstab(col, "label")
    
        tempdf = tempdf.filter(tempdf("0")>4  && tempdf("-1")>0) 
        tempdf = tempdf.withColumn("ratio", tempdf("1")/tempdf("0"))
        tempdf = tempdf.filter(tempdf("ratio")<1) 
        tempdf = tempdf.coalesce(1)
        
        var savepath = "xrli/credit/highrisk_tables/" + col + ".csv"
        val saveOptions = Map("header" -> "true", "path" -> savepath)
        tempdf.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
   
        
//      println(Trans_ori_df.stat.corr(col, "label"))
    }
    
    //labeledData.stat.freqItems(Arr_dist, 0.5).show()
    //println(labeledData.stat.approxQuantile("trans_at", Array(0.2,0.4,0.6,0.8), 0.2))
    
    
    
//        labeledData.stat.crosstab("card_media", "cross_dist_in").show
//    
//    val card_media1 = labeledData.filter(labeledData("card_media").===(1)) 
//    val card_media2 = labeledData.filter(labeledData("card_media").===(2))
//    val card_media3 = labeledData.filter(labeledData("card_media").===(3))
//    val card_media4 = labeledData.filter(labeledData("card_media").===(4))
//    val card_media5 = labeledData.filter(labeledData("card_media").===(5))
//    
//    println("card_media:" + "1: " + card_media1.count + "  2: " + card_media2.count + "  3: " + card_media3.count + "  4: " + card_media4.count + "  5: " + card_media5.count)
//    
//    val cross_dist0 = labeledData.filter(labeledData("cross_dist_in").===(0))   //不跨境
//    val cross_dist1 = labeledData.filter(labeledData("cross_dist_in").===(1))  //跨境
//    println("cross_dist:" + "0: " + cross_dist0.count + "  1: " + cross_dist1.count)
    
    
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  

    
}