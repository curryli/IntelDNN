package PropMap

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


object vectorization {
  def main(args: Array[String]): Unit = {

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
    
    
    var transdata = sql(s"select settle_tp,settle_cycle,block_id,trans_fwd_st,trans_rcv_st,sms_dms_conv_in,cross_dist_in,tfr_in_in,trans_md,source_region_cd,dest_region_cd,cups_card_in,cups_sig_card_in,card_class,card_attr,acq_ins_tp,fwd_ins_tp,rcv_ins_tp,iss_ins_tp,acpt_ins_tp,resp_cd1,resp_cd2,resp_cd3,resp_cd4,cu_trans_st,sti_takeout_in,trans_id,trans_tp,trans_chnl,card_media,card_brand,trans_id_conv,trans_curr_cd,conn_md,msg_tp,msg_tp_conv,card_bin,related_card_bin,trans_proc_cd,trans_proc_cd_conv,mchnt_tp,pos_entry_md_cd,card_seq,pos_cond_cd,pos_cond_cd_conv,term_tp,rsn_cd,addn_pos_inf,orig_msg_tp,orig_msg_tp_conv,related_trans_id,related_trans_chnl,orig_trans_id,orig_trans_chnl,orig_card_media,spec_settle_in,iss_ds_settle_in,acq_ds_settle_in,upd_in,exp_rsn_cd,pri_cycle_no,corr_pri_cycle_no,disc_in,orig_disc_curr_cd,fwd_settle_conv_rt,rcv_settle_conv_rt,fwd_settle_curr_cd,rcv_settle_curr_cd,sp_mchnt_cd,trans_media,mchnt_cd, " +
      s"substring(acq_ins_id_cd,0,4) as acq_ins_id_cd_BK, substring(acq_ins_id_cd,5,8) as acq_ins_id_cd_RG, " + 
      s"substring(fwd_ins_id_cd,0,4) as fwd_ins_id_cd_BK, substring(fwd_ins_id_cd,5,8) as fwd_ins_id_cd_RG, " + 
      s"substring(rcv_ins_id_cd,0,4) as rcv_ins_id_cd_BK, substring(rcv_ins_id_cd,5,8) as rcv_ins_id_cd_RG, " + 
      s"substring(iss_ins_id_cd,0,4) as iss_ins_id_cd_BK, substring(iss_ins_id_cd,5,8) as iss_ins_id_cd_RG, " + 
      s"substring(related_ins_id_cd,0,4) as related_ins_id_cd_BK, substring(related_ins_id_cd,5,8) as related_ins_id_cd_RG, " + 
      s"substring(acpt_ins_id_cd,0,4) acpt_ins_id_cd_BK, substring(acpt_ins_id_cd,5,8) as acpt_ins_id_cd_RG, " + 
      s"substring(settle_fwd_ins_id_cd,0,4) as settle_fwd_ins_id_cd_BK, substring(settle_fwd_ins_id_cd,5,8) as settle_fwd_ins_id_cd_RG, " + 
      s"substring(settle_rcv_ins_id_cd,0,4) as settle_rcv_ins_id_cd_BK, substring(settle_rcv_ins_id_cd,5,8) as settle_rcv_ins_id_cd_RG, " + 
      s"substring(acct_ins_id_cd,0,4) as acct_ins_id_cd_BK, substring(acct_ins_id_cd,5,8) as acct_ins_id_cd_RG " +
	    s"from tbl_common_his_trans where pdate=20160701") 
  
	    println(transdata.count())
	    
      //val a = transdata.select("settle_tp","settle_cycle")
      val mapedresult = transdata.map(f => (MapHash(f(0).toString),MapHash(f(1).toString),MapHash(f(2).toString),MapHash(f(3).toString),MapHash(f(4).toString),MapHash(f(5).toString)))
      mapedresult.show(5)
  
   }
  
   def MapHash(str:String) :Long ={
     if(str.isEmpty())
        -1
     else
       BKDRHash(str)
     
   }
  
   def BKDRHash(str:String) :Long ={
     val seed:Long  = 131 // 31 131 1313 13131 131313 etc..
     var hash:Long  = 0
       for(i <- 0 to str.length-1){
        hash = hash * seed + str.charAt(i)
        hash = hash.&("8388607".toLong)        //0x7FFFFF              //固定一下长度
       }
     return hash 
   }

}