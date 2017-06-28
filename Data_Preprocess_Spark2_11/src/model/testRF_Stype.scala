package model
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


object testRF_Stype {
  val QD_money_num = 10
  val QD_disc_num = 10
   

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
     
    var input_dir = rangedir + "Labeled_All"
    var labeledData = IntelUtil.get_from_HDFS.get_labeled_DF(ss, input_dir).persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    labeledData.show(5)
 
    println("testData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
      
    var vec_data = labeledData
    
    val udf_transid_type = udf[String, String]{xstr => xstr.substring(0,1)}
    //vec_data = vec_data.filter(udf_transid_type(vec_data("trans_id_filled"))==="S")
    
 
    
    val QD_money = new QuantileDiscretizer().setInputCol("trans_at_filled").setOutputCol("trans_at_QD").setNumBuckets(QD_money_num)
    val QD_disc = new QuantileDiscretizer().setInputCol("total_disc_at_filled").setOutputCol("disc_at_QD").setNumBuckets(QD_disc_num)
   
    val QD_money_model = QD_money.fit(vec_data)
    val QD_disc_model = QD_disc.fit(vec_data)
    
    vec_data = QD_money_model.transform(vec_data)
    vec_data = QD_disc_model.transform(vec_data)
    vec_data.show(5)
    
    val woeMap_trans_at_QD = IntelUtil.funUtil.get_woe_map(vec_data, "trans_at_QD")
    vec_data = IntelUtil.funUtil.WOE_modify(vec_data, "trans_at_QD", woeMap_trans_at_QD)
     
    val woeMap_disc_at_QD = IntelUtil.funUtil.get_woe_map(vec_data, "disc_at_QD")
    vec_data = IntelUtil.funUtil.WOE_modify(vec_data, "disc_at_QD", woeMap_disc_at_QD)
    
    vec_data.show(50)
    
    
     //该数组李必须都是doubletype，否则VectorAssembler报错
    val CatVecArr =  Array("day_week_filled","hour_filled", "trans_at_filled", "total_disc_at_filled","trans_at_QD", "disc_at_QD")
    //val CatVecArr =  Array("day_week_filled","hour_filled","tfr_dt_tm_filled","trans_at_filled", "total_disc_at_filled")
     //val CatVecArr =  Array("day_week_filled","hour_filled", "trans_at_filled", "total_disc_at_filled")
     //val CatVecArr = Array("day_week_filled","hour_filled", "trans_at_filled", "total_disc_at_filled", "settle_tp_CatVec","settle_cycle_CatVec","block_id_CatVec","trans_fwd_st_CatVec","trans_rcv_st_CatVec","sms_dms_conv_in_CatVec","cross_dist_in_CatVec","tfr_in_in_CatVec","trans_md_CatVec","source_region_cd_CatVec","dest_region_cd_CatVec","cups_card_in_CatVec","cups_sig_card_in_CatVec","card_class_CatVec","card_attr_CatVec","acq_ins_tp_CatVec","fwd_ins_tp_CatVec","rcv_ins_tp_CatVec","iss_ins_tp_CatVec","acpt_ins_tp_CatVec","resp_cd1_CatVec","resp_cd2_CatVec","resp_cd3_CatVec","resp_cd4_CatVec","cu_trans_st_CatVec","sti_takeout_in_CatVec","trans_id_CatVec","trans_tp_CatVec","trans_chnl_CatVec","card_media_CatVec","card_brand_CatVec","trans_id_conv_CatVec","trans_curr_cd_CatVec","conn_md_CatVec","msg_tp_CatVec","msg_tp_conv_CatVec","trans_proc_cd_CatVec","trans_proc_cd_conv_CatVec","mchnt_tp_CatVec","pos_entry_md_cd_CatVec","card_seq_CatVec","pos_cond_cd_CatVec","pos_cond_cd_conv_CatVec","term_tp_CatVec","rsn_cd_CatVec","addn_pos_inf_CatVec","orig_msg_tp_CatVec","orig_msg_tp_conv_CatVec","related_trans_id_CatVec","related_trans_chnl_CatVec","orig_trans_id_CatVec","orig_trans_chnl_CatVec","orig_card_media_CatVec","spec_settle_in_CatVec","iss_ds_settle_in_CatVec","acq_ds_settle_in_CatVec","upd_in_CatVec","exp_rsn_cd_CatVec","pri_cycle_no_CatVec","corr_pri_cycle_no_CatVec","disc_in_CatVec","orig_disc_curr_cd_CatVec","fwd_settle_conv_rt_CatVec","rcv_settle_conv_rt_CatVec","fwd_settle_curr_cd_CatVec","rcv_settle_curr_cd_CatVec","sp_mchnt_cd_CatVec","acq_ins_id_cd_BK_CatVec","acq_ins_id_cd_RG_CatVec","fwd_ins_id_cd_BK_CatVec","fwd_ins_id_cd_RG_CatVec","rcv_ins_id_cd_BK_CatVec","rcv_ins_id_cd_RG_CatVec","iss_ins_id_cd_BK_CatVec","iss_ins_id_cd_RG_CatVec","related_ins_id_cd_BK_CatVec","related_ins_id_cd_RG_CatVec","acpt_ins_id_cd_BK_CatVec","acpt_ins_id_cd_RG_CatVec","settle_fwd_ins_id_cd_BK_CatVec","settle_fwd_ins_id_cd_RG_CatVec","settle_rcv_ins_id_cd_BK_CatVec","settle_rcv_ins_id_cd_RG_CatVec","acct_ins_id_cd_BK_CatVec","acct_ins_id_cd_RG_CatVec")    
  
    
    val assembler = new VectorAssembler()
      .setInputCols(CatVecArr)
      .setOutputCol("featureVector")
    
    val label_indexer = new StringIndexer()
     .setInputCol("label_filled")
     .setOutputCol("label_idx")
     .fit(vec_data)  
       
      
    val rfClassifier = new RandomForestClassifier()
        .setLabelCol("label_idx")
        .setFeaturesCol("featureVector")
        .setNumTrees(10)
        .setMaxBins(10000)
       .setThresholds(Array(10,1)) 
     
      
      val pipeline = new Pipeline().setStages(Array(assembler,label_indexer, rfClassifier))
      
      val model = pipeline.fit(vec_data)
      
      var test_data = IntelUtil.get_from_HDFS.get_labeled_DF(ss, "xrli/IntelDNN/Counterfeit/201607/Labeled_All") 
       
      test_data  = test_data.filter(udf_transid_type(test_data("trans_id_filled"))==="S")
    
       
      println("testData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
      test_data = QD_money_model.transform(test_data)
      test_data = QD_disc_model.transform(test_data)
      test_data.show(5)
    
      test_data = IntelUtil.funUtil.WOE_modify(test_data, "trans_at_QD", woeMap_trans_at_QD)
      test_data = IntelUtil.funUtil.WOE_modify(test_data, "disc_at_QD", woeMap_disc_at_QD)
      test_data.show(5)
      
//      println("start index test data!")
//      var test_07_indexed = my_index_Model.transform(test_07)
//      println("Indexed done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
//      test_07_indexed.show(5)
       
      val predictionResultDF_07 = model.transform(test_data)
        
      val eval_result = IntelUtil.funUtil.get_CF_Matrix(predictionResultDF_07)
       
     println("Current Precision_P is: " + eval_result.Precision_P)
     println("Current Recall_P is: " + eval_result.Recall_P)
     
     
 
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
   
     
  }
  
  
  
    
}