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
import Algorithm._
import scala.collection.mutable.MutableList
import scala.Range
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import Algorithm._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.Normalizer


object Hash_vectorization {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);
    
   val conf = new SparkConf().setAppName("compare_2time")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
     
    var transdata = hc.sql(s"select settle_tp,settle_cycle,block_id,trans_fwd_st,trans_rcv_st,sms_dms_conv_in,cross_dist_in,tfr_in_in,trans_md,source_region_cd,dest_region_cd,cups_card_in,cups_sig_card_in,card_class,card_attr,acq_ins_tp,fwd_ins_tp,rcv_ins_tp,iss_ins_tp,acpt_ins_tp,resp_cd1,resp_cd2,resp_cd3,resp_cd4,cu_trans_st,sti_takeout_in,trans_id,trans_tp,trans_chnl,card_media,card_brand,trans_id_conv,trans_curr_cd,conn_md,msg_tp,msg_tp_conv,card_bin,related_card_bin,trans_proc_cd,trans_proc_cd_conv,mchnt_tp,pos_entry_md_cd,card_seq,pos_cond_cd,pos_cond_cd_conv,term_tp,rsn_cd,addn_pos_inf,orig_msg_tp,orig_msg_tp_conv,related_trans_id,related_trans_chnl,orig_trans_id,orig_trans_chnl,orig_card_media,spec_settle_in,iss_ds_settle_in,acq_ds_settle_in,upd_in,exp_rsn_cd,pri_cycle_no,corr_pri_cycle_no,disc_in,orig_disc_curr_cd,fwd_settle_conv_rt,rcv_settle_conv_rt,fwd_settle_curr_cd,rcv_settle_curr_cd,sp_mchnt_cd,trans_media,mchnt_cd, " +
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
  
	    println(transdata.columns.length)
	     
	    
//	    //scala2.10  通过是使用case class的方式，不过在scala 2.10中最大支持22个字段的case class,
//      val mapedresult = transdata.map(f => (MapHash(f(0).toString),MapHash(f(1).toString),MapHash(f(2).toString),MapHash(f(3).toString),MapHash(f(4).toString),
//                  MapHash(f(5).toString),MapHash(f(6).toString),MapHash(f(7).toString),MapHash(f(8).toString),MapHash(f(9).toString),MapHash(f(10).toString),
//                  MapHash(f(11).toString),MapHash(f(12).toString),MapHash(f(13).toString),MapHash(f(14).toString),MapHash(f(15).toString),MapHash(f(16).toString),
//                  MapHash(f(17).toString),MapHash(f(18).toString),MapHash(f(19).toString),MapHash(f(20).toString),MapHash(f(21).toString)
//      ))
//      mapedresult.take(5).foreach(println)
  
//	    val toLong = udf[Long, Double](_.toLong)
//	    transdata.withColumn("settle_tp_HS", toLong(transdata("click")))
   
      val udf_MapHash = udf[Long, String]{xstr => 
        if(xstr.isEmpty())
          -1
        else
         BKDRHash(xstr)
      }

//      transdata = transdata.withColumn("settle_tp_HS", udf_MapHash(transdata("settle_tp")))
//      println(transdata.columns.length)
//      transdata.show(2)
   
      val DisperseArr = Array("settle_tp","settle_cycle","block_id","trans_fwd_st","trans_rcv_st","sms_dms_conv_in","cross_dist_in","tfr_in_in","trans_md","source_region_cd","dest_region_cd","cups_card_in","cups_sig_card_in","card_class","card_attr","acq_ins_tp","fwd_ins_tp","rcv_ins_tp","iss_ins_tp","acpt_ins_tp","resp_cd1","resp_cd2","resp_cd3","resp_cd4","cu_trans_st","sti_takeout_in","trans_id","trans_tp","trans_chnl","card_media","card_brand","trans_id_conv","trans_curr_cd","conn_md","msg_tp","msg_tp_conv","card_bin","related_card_bin","trans_proc_cd","trans_proc_cd_conv","mchnt_tp","pos_entry_md_cd","card_seq","pos_cond_cd","pos_cond_cd_conv","term_tp","rsn_cd","addn_pos_inf","orig_msg_tp","orig_msg_tp_conv","related_trans_id","related_trans_chnl","orig_trans_id","orig_trans_chnl","orig_card_media","spec_settle_in","iss_ds_settle_in","acq_ds_settle_in","upd_in","exp_rsn_cd","pri_cycle_no","corr_pri_cycle_no","disc_in","orig_disc_curr_cd","fwd_settle_conv_rt","rcv_settle_conv_rt","fwd_settle_curr_cd","rcv_settle_curr_cd","sp_mchnt_cd","trans_media","mchnt_cd","acq_ins_id_cd_BK","acq_ins_id_cd_RG","fwd_ins_id_cd_BK","fwd_ins_id_cd_RG","rcv_ins_id_cd_BK","rcv_ins_id_cd_RG","iss_ins_id_cd_BK","iss_ins_id_cd_RG","related_ins_id_cd_BK","related_ins_id_cd_RG","acpt_ins_id_cd_BK","acpt_ins_id_cd_RG","settle_fwd_ins_id_cd_BK","settle_fwd_ins_id_cd_RG","settle_rcv_ins_id_cd_BK","settle_rcv_ins_id_cd_RG","acct_ins_id_cd_BK","acct_ins_id_cd_RG")
      for(oldcol <- DisperseArr){
        val newcol = oldcol + "_HS" 
        transdata = transdata.withColumn(newcol, udf_MapHash(transdata(oldcol))) 
      }
     println("Hashed dataframe")
     transdata.show(5)
     
//     val normalizer2 = new Normalizer().setInputCol("settle_tp_HS").setOutputCol("settle_tp_HS_Norm")     //默认是L2
//     transdata = normalizer2.transform(transdata)
     
     import org.apache.spark.ml.feature.VectorAssembler
     val HashArr = Array("settle_tp_HS","settle_cycle_HS","block_id_HS","trans_fwd_st_HS","trans_rcv_st_HS","sms_dms_conv_in_HS","cross_dist_in_HS","tfr_in_in_HS","trans_md_HS","source_region_cd_HS","dest_region_cd_HS","cups_card_in_HS","cups_sig_card_in_HS","card_class_HS","card_attr_HS","acq_ins_tp_HS","fwd_ins_tp_HS","rcv_ins_tp_HS","iss_ins_tp_HS","acpt_ins_tp_HS","resp_cd1_HS","resp_cd2_HS","resp_cd3_HS","resp_cd4_HS","cu_trans_st_HS","sti_takeout_in_HS","trans_id_HS","trans_tp_HS","trans_chnl_HS","card_media_HS","card_brand_HS","trans_id_conv_HS","trans_curr_cd_HS","conn_md_HS","msg_tp_HS","msg_tp_conv_HS","card_bin_HS","related_card_bin_HS","trans_proc_cd_HS","trans_proc_cd_conv_HS","mchnt_tp_HS","pos_entry_md_cd_HS","card_seq_HS","pos_cond_cd_HS","pos_cond_cd_conv_HS","term_tp_HS","rsn_cd_HS","addn_pos_inf_HS","orig_msg_tp_HS","orig_msg_tp_conv_HS","related_trans_id_HS","related_trans_chnl_HS","orig_trans_id_HS","orig_trans_chnl_HS","orig_card_media_HS","spec_settle_in_HS","iss_ds_settle_in_HS","acq_ds_settle_in_HS","upd_in_HS","exp_rsn_cd_HS","pri_cycle_no_HS","corr_pri_cycle_no_HS","disc_in_HS","orig_disc_curr_cd_HS","fwd_settle_conv_rt_HS","rcv_settle_conv_rt_HS","fwd_settle_curr_cd_HS","rcv_settle_curr_cd_HS","sp_mchnt_cd_HS","trans_media_HS","mchnt_cd_HS","acq_ins_id_cd_BK_HS","acq_ins_id_cd_RG_HS","fwd_ins_id_cd_BK_HS","fwd_ins_id_cd_RG_HS","rcv_ins_id_cd_BK_HS","rcv_ins_id_cd_RG_HS","iss_ins_id_cd_BK_HS","iss_ins_id_cd_RG_HS","related_ins_id_cd_BK_HS","related_ins_id_cd_RG_HS","acpt_ins_id_cd_BK_HS","acpt_ins_id_cd_RG_HS","settle_fwd_ins_id_cd_BK_HS","settle_fwd_ins_id_cd_RG_HS","settle_rcv_ins_id_cd_BK_HS","settle_rcv_ins_id_cd_RG_HS","acct_ins_id_cd_BK_HS","acct_ins_id_cd_RG_HS")
     val assembler1 = new VectorAssembler()
      .setInputCols(HashArr)
      .setOutputCol("features_HS")
     
     var vec_data = assembler1.transform(transdata)
     //println("Vectorized dataframe")
     vec_data.show(5)
     
     val normalizer2 = new Normalizer().setInputCol("features_HS").setOutputCol("normFeatures")     //默认是L2
     val l2NormData = normalizer2.transform(vec_data)
     //println("Normalize dataframe")
     l2NormData.show(5)
     
//     l2NormData.select("features_HS").show(5)
//     l2NormData.select("normFeatures").show(5)
       
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
        hash = hash * seed + str.charAt(i)   //最终映射结果是原字符长度*2+1
        //hash = hash.&("8388607".toLong)        //0x7FFFFF              //固定一下长度
       }
     return hash 
   }

}