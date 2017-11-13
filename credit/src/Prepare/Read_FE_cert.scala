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



object Read_FE_cert {

  
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
 
    val train_ori_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/train_trans_encrypt.csv")
    val test_ori_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/test_trans_encrypt.csv")
     
    var Trans_ori_df =  train_ori_df.unionAll(test_ori_df)
    
//    Trans_ori_df = Trans_ori_df.sample(false, 0.001)
    
    val date_No_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/DateDicts.csv")
    
    date_No_df.dtypes.foreach(println)
    
    var label_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/train_label_encrypt.csv")
    
    label_df = label_df.select(label_df("certid").as("certid_label"), label_df("label"))
    
    label_df.dtypes.foreach(println)
    
    Trans_ori_df = Trans_ori_df.na.fill(-1)
  
   
    Trans_ori_df = Trans_ori_df.join(date_No_df, Trans_ori_df("Settle_dt")===date_No_df("Settle_dt"), "left_outer").drop(Trans_ori_df("Settle_dt"))
    
    Trans_ori_df = Trans_ori_df.join(label_df, Trans_ori_df("certid")===label_df("certid_label"), "left_outer").drop("certid_label")
     
    Trans_ori_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    println("get labeledData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
      
    //labeledData = Prepare.FeatureEngineer_function.FE_function(ss, labeledData)
    var new_labeled = FE_new.FE_function(ss, Trans_ori_df).persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    Trans_ori_df.unpersist(blocking=false)
    
    println("FeatureEngineer_function done in "  + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
    
    var new_labeled_cols = new_labeled.columns 
    
    println(new_labeled_cols.mkString(","))
    
//    var savepath = "xrli/credit/trans_FE"
//    val saveOptions = Map("header" -> "false", "path" -> savepath)
//    new_labeled.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    
    
    var ori_idx = new_labeled_cols.toList.toSet   ///.dropRight(1)
  
    
    var Arr_to_idx = catgory_list
    val cat_filled_list = Arr_to_idx.map { x => x + "_filled"}
    val CatVecArr = cat_filled_list.map { x => x + "_idx"}

     
    val no_idx_arr = (ori_idx.diff(Arr_to_idx.toSet)).diff(not_train_list.toSet).toArray   
    
    val train_arr  = no_idx_arr.++:(CatVecArr)
 
    
    val save_arr = train_arr.+:("certid").+:("card_no").+:("label")
 
    println(train_arr.mkString(","))
    
    new_labeled = new_labeled.na.fill(-1)
    new_labeled.show(5)
    
    val udf_replaceEmpty = udf[String, String]{xstr => 
    
    var result = "NANs"
    try{
        if(xstr.isEmpty())
          result = "NANs"
        else
          result = xstr
      }
     catch{
       case ex: java.lang.NullPointerException => {result = "NULLs"}
     }       
      result
     }
      
       
     for(oldcol <- Arr_to_idx){
        val newcol = oldcol + "_filled" 
       new_labeled = new_labeled.withColumn(newcol, udf_replaceEmpty(new_labeled(oldcol)))
     }
     println("NaNs filled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
    
      
    val pipeline_idx = new Pipeline().setStages(IntelUtil.funUtil.Multi_idx_Pipeline(cat_filled_list).toArray)

    var idxedData = pipeline_idx.fit(new_labeled).transform(new_labeled)
    println("idx done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
    
    idxedData = idxedData.drop("Trans_at")
     
    //idxedData.show(5)
      
     
   //////////////////////////////////////////save//////////////////////////////////////// 
    println(idxedData.columns.mkString(","))
    //idxedData.dtypes.foreach(println)
        
    var save_DF = idxedData.selectExpr(save_arr:_*)

    
    
//    var stat_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/stat_DF.csv")
//    stat_df = stat_df.drop("label")
//    stat_df.show(5)
//    
//    save_DF = save_DF.join(stat_df, save_DF("certid")===stat_df("certid_stat"), "left_outer").drop("certid_stat")
    
    //save_DF.show(5)
    
    println(save_DF.columns.mkString(","))
        
    save_DF.rdd.map(_.mkString(",")).saveAsTextFile("xrli/credit/cert_all_right")
    
    println("save done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
     ////////////////////////////////////////////////////////////////////////////////////////// 
   
  
 
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
    
    
    
    
    
     
  }
  
  

    
}