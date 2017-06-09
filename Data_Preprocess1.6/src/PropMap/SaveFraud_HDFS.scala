package PropMap
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.PartitionStrategy
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


object SaveFraud_HDFS {
  

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);

    //    require(args.length == 3)

    val conf = new SparkConf().setAppName("SaveFraud_HDFS")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
 
    val startTime = System.currentTimeMillis(); 
    
    //注意select * 会出现内存溢出报错，估计是1行太多了，java堆栈不够，起始可以设置  http://blog.csdn.net/oaimm/article/details/25298691  但这里就少选几列就可以了
    var FraudData = hc.sql(s"select t1.tfr_dt_tm, t1.trans_at, t1.total_disc_at, "+
      s"t1.settle_tp,t1.settle_cycle,t1.block_id,t1.trans_fwd_st,t1.trans_rcv_st,t1.sms_dms_conv_in,t1.cross_dist_in,t1.tfr_in_in,t1.trans_md,t1.source_region_cd,t1.dest_region_cd,t1.cups_card_in,t1.cups_sig_card_in,t1.card_class,t1.card_attr,t1.acq_ins_tp,t1.fwd_ins_tp,t1.rcv_ins_tp,t1.iss_ins_tp,t1.acpt_ins_tp,t1.resp_cd1,t1.resp_cd2,t1.resp_cd3,t1.resp_cd4,t1.cu_trans_st,t1.sti_takeout_in,t1.trans_id,t1.trans_tp,t1.trans_chnl,t1.card_media,t1.card_brand,t1.trans_id_conv,t1.trans_curr_cd,t1.conn_md,t1.msg_tp,t1.msg_tp_conv,t1.card_bin,t1.related_card_bin,t1.trans_proc_cd,t1.trans_proc_cd_conv,t1.mchnt_tp,t1.pos_entry_md_cd,t1.card_seq,t1.pos_cond_cd,t1.pos_cond_cd_conv,t1.term_tp,t1.rsn_cd,t1.addn_pos_inf,t1.orig_msg_tp,t1.orig_msg_tp_conv,t1.related_trans_id,t1.related_trans_chnl,t1.orig_trans_id,t1.orig_trans_chnl,t1.orig_card_media,t1.spec_settle_in,t1.iss_ds_settle_in,t1.acq_ds_settle_in,t1.upd_in,t1.exp_rsn_cd,t1.pri_cycle_no,t1.corr_pri_cycle_no,t1.disc_in,t1.orig_disc_curr_cd,t1.fwd_settle_conv_rt,t1.rcv_settle_conv_rt,t1.fwd_settle_curr_cd,t1.rcv_settle_curr_cd,t1.sp_mchnt_cd, " +    //mchnt_cd,t1.
      s"substring(t1.acq_ins_id_cd,1,4) as acq_ins_id_cd_BK, substring(t1.acq_ins_id_cd,5,4) as acq_ins_id_cd_RG, " + 
      s"substring(t1.fwd_ins_id_cd,1,4) as fwd_ins_id_cd_BK, substring(t1.fwd_ins_id_cd,5,4) as fwd_ins_id_cd_RG, " + 
      s"substring(t1.rcv_ins_id_cd,1,4) as rcv_ins_id_cd_BK, substring(t1.rcv_ins_id_cd,5,4) as rcv_ins_id_cd_RG, " + 
      s"substring(t1.iss_ins_id_cd,1,4) as iss_ins_id_cd_BK, substring(t1.iss_ins_id_cd,5,4) as iss_ins_id_cd_RG, " + 
      s"substring(t1.related_ins_id_cd,1,4) as related_ins_id_cd_BK, substring(t1.related_ins_id_cd,5,4) as related_ins_id_cd_RG, " + 
      s"substring(t1.acpt_ins_id_cd,1,4) acpt_ins_id_cd_BK, substring(t1.acpt_ins_id_cd,5,4) as acpt_ins_id_cd_RG, " + 
      s"substring(t1.settle_fwd_ins_id_cd,1,4) as settle_fwd_ins_id_cd_BK, substring(t1.settle_fwd_ins_id_cd,5,4) as settle_fwd_ins_id_cd_RG, " + 
      s"substring(t1.settle_rcv_ins_id_cd,1,4) as settle_rcv_ins_id_cd_BK, substring(t1.settle_rcv_ins_id_cd,5,4) as settle_rcv_ins_id_cd_RG, " + 
      s"substring(t1.acct_ins_id_cd,1,4) as acct_ins_id_cd_BK, substring(t1.acct_ins_id_cd,5,4) as acct_ins_id_cd_RG " +
      s"from tbl_common_his_trans t1 "+
      s"left semi join tbl_arsvc_fraud_trans t2 "+
      s"on (t1.sys_tra_no=t2.sys_tra_no and t1.pri_acct_no_conv=t2.ar_pri_acct_no and t1.mchnt_cd=t2.mchnt_cd and t1.pdate=t2.trans_dt) "+
      s"where t1.pdate>=20160701 and t1.pdate<=20160701 ")//.repartition(1000).persist(StorageLevel.MEMORY_AND_DISK_SER)  //.cache 
        
    
      println("FraudData.count(): " + FraudData.count())
      FraudData.rdd.map { x => x.toSeq.mkString("\t") }.saveAsTextFile("xrli/IntelDNN/Fraud_in_common_trans")
 
  }
  
  
  
  
}