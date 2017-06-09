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

object LabelSamples {
  

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);

    //    require(args.length == 3)

    val conf = new SparkConf().setAppName("countDistinctProp")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
   
//   hc.sql(s"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat" +
//           //s"set mapred.max.split.size=10240000000" +
//           s"set mapred.min.split.size.per.node=10240000000" +
//           s"set mapred.min.split.size.per.rack=10240000000" +
//           s"set mapreduce.jobtracker.split.metainfo.maxsize = -1" +
//           s"set mapreduce.job.queuename=root.default")
           
    
    //注意select * 会出现内存溢出报错，估计是1行太多了，java堆栈不够，起始可以设置  http://blog.csdn.net/oaimm/article/details/25298691  但这里就少选几列就可以了
    var AllData = hc.sql(s"select settle_tp,settle_cycle,block_id,trans_fwd_st,trans_rcv_st,sms_dms_conv_in,cross_dist_in,tfr_in_in,trans_md,source_region_cd,dest_region_cd,cups_card_in,cups_sig_card_in,card_class,card_attr,acq_ins_tp,fwd_ins_tp,rcv_ins_tp,iss_ins_tp,acpt_ins_tp,resp_cd1,resp_cd2,resp_cd3,resp_cd4,cu_trans_st,sti_takeout_in,trans_id,trans_tp,trans_chnl,card_media,card_brand,trans_id_conv,trans_curr_cd,conn_md,msg_tp,msg_tp_conv,card_bin,related_card_bin,trans_proc_cd,trans_proc_cd_conv,mchnt_tp,pos_entry_md_cd,card_seq,pos_cond_cd,pos_cond_cd_conv,term_tp,rsn_cd,addn_pos_inf,orig_msg_tp,orig_msg_tp_conv,related_trans_id,related_trans_chnl,orig_trans_id,orig_trans_chnl,orig_card_media,spec_settle_in,iss_ds_settle_in,acq_ds_settle_in,upd_in,exp_rsn_cd,pri_cycle_no,corr_pri_cycle_no,disc_in,orig_disc_curr_cd,fwd_settle_conv_rt,rcv_settle_conv_rt,fwd_settle_curr_cd,rcv_settle_curr_cd,sp_mchnt_cd,trans_media, " +    //mchnt_cd,
      s"substring(acq_ins_id_cd,1,4) as acq_ins_id_cd_BK, substring(acq_ins_id_cd,5,4) as acq_ins_id_cd_RG, " + 
      s"substring(fwd_ins_id_cd,1,4) as fwd_ins_id_cd_BK, substring(fwd_ins_id_cd,5,4) as fwd_ins_id_cd_RG, " + 
      s"substring(rcv_ins_id_cd,1,4) as rcv_ins_id_cd_BK, substring(rcv_ins_id_cd,5,4) as rcv_ins_id_cd_RG, " + 
      s"substring(iss_ins_id_cd,1,4) as iss_ins_id_cd_BK, substring(iss_ins_id_cd,5,4) as iss_ins_id_cd_RG, " + 
      s"substring(related_ins_id_cd,1,4) as related_ins_id_cd_BK, substring(related_ins_id_cd,5,4) as related_ins_id_cd_RG, " + 
      s"substring(acpt_ins_id_cd,1,4) acpt_ins_id_cd_BK, substring(acpt_ins_id_cd,5,4) as acpt_ins_id_cd_RG, " + 
      s"substring(settle_fwd_ins_id_cd,1,4) as settle_fwd_ins_id_cd_BK, substring(settle_fwd_ins_id_cd,5,4) as settle_fwd_ins_id_cd_RG, " + 
      s"substring(settle_rcv_ins_id_cd,1,4) as settle_rcv_ins_id_cd_BK, substring(settle_rcv_ins_id_cd,5,4) as settle_rcv_ins_id_cd_RG, " + 
      s"substring(acct_ins_id_cd,1,4) as acct_ins_id_cd_BK, substring(acct_ins_id_cd,5,4) as acct_ins_id_cd_RG " +
	    s"from tbl_common_his_trans where pdate>=20160701 and pdate<=20160703").cache           //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    println("AllData count is " + AllData.count())
    
    var FraudData = hc.sql(s"select t1.settle_tp,t1.settle_cycle,t1.block_id,t1.trans_fwd_st,t1.trans_rcv_st,t1.sms_dms_conv_in,t1.cross_dist_in,t1.tfr_in_in,t1.trans_md,t1.source_region_cd,t1.dest_region_cd,t1.cups_card_in,t1.cups_sig_card_in,t1.card_class,t1.card_attr,t1.acq_ins_tp,t1.fwd_ins_tp,t1.rcv_ins_tp,t1.iss_ins_tp,t1.acpt_ins_tp,t1.resp_cd1,t1.resp_cd2,t1.resp_cd3,t1.resp_cd4,t1.cu_trans_st,t1.sti_takeout_in,t1.trans_id,t1.trans_tp,t1.trans_chnl,t1.card_media,t1.card_brand,t1.trans_id_conv,t1.trans_curr_cd,t1.conn_md,t1.msg_tp,t1.msg_tp_conv,t1.card_bin,t1.related_card_bin,t1.trans_proc_cd,t1.trans_proc_cd_conv,t1.mchnt_tp,t1.pos_entry_md_cd,t1.card_seq,t1.pos_cond_cd,t1.pos_cond_cd_conv,t1.term_tp,t1.rsn_cd,t1.addn_pos_inf,t1.orig_msg_tp,t1.orig_msg_tp_conv,t1.related_trans_id,t1.related_trans_chnl,t1.orig_trans_id,t1.orig_trans_chnl,t1.orig_card_media,t1.spec_settle_in,t1.iss_ds_settle_in,t1.acq_ds_settle_in,t1.upd_in,t1.exp_rsn_cd,t1.pri_cycle_no,t1.corr_pri_cycle_no,t1.disc_in,t1.orig_disc_curr_cd,t1.fwd_settle_conv_rt,t1.rcv_settle_conv_rt,t1.fwd_settle_curr_cd,t1.rcv_settle_curr_cd,t1.sp_mchnt_cd,t1.trans_media, " +    //mchnt_cd,t1.
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
      s"where t1.pdate>=20160701 and t1.pdate<=20160703")
    println("FraudData count is " + FraudData.count())
    
    var NormalData = AllData.except(FraudData)
    println("NormalData count is " + NormalData.count())
    
    val udf_Map0 = udf[Int, String]{xstr => 0}
    val udf_Map1 = udf[Int, String]{xstr => 1}
    
    FraudData = FraudData.withColumn("isFraud", udf_Map1(FraudData("trans_md")))
    FraudData.show(5)
    NormalData = NormalData.withColumn("isFraud", udf_Map0(NormalData("trans_md")))
    NormalData.show(5)
    
    var LabeledData = NormalData.unionAll(FraudData)
    //LabeledData.saveAsTable("LabeledData_0701")       //LabeledData.rdd....
    
    println("LabeledData.filter Fraud : " + LabeledData.filter(LabeledData("isFraud").===(1)))
    println("LabeledData.filter Normal : " + LabeledData.filter(LabeledData("isFraud").===(0)))
    
  }
  
  
  
  
  
  
}