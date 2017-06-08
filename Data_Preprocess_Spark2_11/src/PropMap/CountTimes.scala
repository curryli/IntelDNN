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


object CountTimes {
  

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
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.schema.verification", false)
      .enableHiveSupport()
      .getOrCreate()
  
    import ss.implicits._
    import ss.sql
 
    val startTime = System.currentTimeMillis(); 
     
    var AllData = sql(s"select pri_acct_no_conv, tfr_dt_tm, trans_at, total_disc_at, "+
      s"settle_tp,settle_cycle,block_id,trans_fwd_st,trans_rcv_st,sms_dms_conv_in,cross_dist_in,tfr_in_in,trans_md,source_region_cd,dest_region_cd,cups_card_in,cups_sig_card_in,card_class,card_attr,acq_ins_tp,fwd_ins_tp,rcv_ins_tp,iss_ins_tp,acpt_ins_tp,resp_cd1,resp_cd2,resp_cd3,resp_cd4,cu_trans_st,sti_takeout_in,trans_id,trans_tp,trans_chnl,card_media,card_brand,trans_id_conv,trans_curr_cd,conn_md,msg_tp,msg_tp_conv,card_bin,related_card_bin,trans_proc_cd,trans_proc_cd_conv,mchnt_tp,pos_entry_md_cd,card_seq,pos_cond_cd,pos_cond_cd_conv,term_tp,rsn_cd,addn_pos_inf,orig_msg_tp,orig_msg_tp_conv,related_trans_id,related_trans_chnl,orig_trans_id,orig_trans_chnl,orig_card_media,spec_settle_in,iss_ds_settle_in,acq_ds_settle_in,upd_in,exp_rsn_cd,pri_cycle_no,corr_pri_cycle_no,disc_in,orig_disc_curr_cd,fwd_settle_conv_rt,rcv_settle_conv_rt,fwd_settle_curr_cd,rcv_settle_curr_cd,sp_mchnt_cd, " +    //mchnt_cd, trans_media,
      s"substring(acq_ins_id_cd,1,4) as acq_ins_id_cd_BK, substring(acq_ins_id_cd,5,4) as acq_ins_id_cd_RG, " + 
      s"substring(fwd_ins_id_cd,1,4) as fwd_ins_id_cd_BK, substring(fwd_ins_id_cd,5,4) as fwd_ins_id_cd_RG, " + 
      s"substring(rcv_ins_id_cd,1,4) as rcv_ins_id_cd_BK, substring(rcv_ins_id_cd,5,4) as rcv_ins_id_cd_RG, " + 
      s"substring(iss_ins_id_cd,1,4) as iss_ins_id_cd_BK, substring(iss_ins_id_cd,5,4) as iss_ins_id_cd_RG, " + 
      s"substring(related_ins_id_cd,1,4) as related_ins_id_cd_BK, substring(related_ins_id_cd,5,4) as related_ins_id_cd_RG, " + 
      s"substring(acpt_ins_id_cd,1,4) acpt_ins_id_cd_BK, substring(acpt_ins_id_cd,5,4) as acpt_ins_id_cd_RG, " + 
      s"substring(settle_fwd_ins_id_cd,1,4) as settle_fwd_ins_id_cd_BK, substring(settle_fwd_ins_id_cd,5,4) as settle_fwd_ins_id_cd_RG, " + 
      s"substring(settle_rcv_ins_id_cd,1,4) as settle_rcv_ins_id_cd_BK, substring(settle_rcv_ins_id_cd,5,4) as settle_rcv_ins_id_cd_RG, " + 
      s"substring(acct_ins_id_cd,1,4) as acct_ins_id_cd_BK, substring(acct_ins_id_cd,5,4) as acct_ins_id_cd_RG " +
	    s"from tbl_common_his_trans where pdate>=20160701 and pdate<=20160701 ").repartition(5000).persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    println("AllData count is " + AllData.count())
    
	  val countdf = AllData.groupBy("pri_acct_no_conv").agg(count("trans_at") as "counts") 
    val filtereddf = countdf.filter(countdf("counts")>5 && countdf("counts")<10)
    println("filtereddf count is " + filtereddf.count())
  }
  
   
}