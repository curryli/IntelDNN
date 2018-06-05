package IntelUtil

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap		


object varUtil { 
  def main(args: Array[String]) { 
  }
		
  val startdate = "20160901"
  val enddate = "20160930"
  val rangeDir = "xrli/IntelDNN/Weika/201609_new/" 
  
 val testDir = "xrli/IntelDNN/Weika/Dir_test/" 
 //val testDir = "xrli/IntelDNN/Weika/Dir_test_all/"
  
  val idx_model = "xrli/IntelDNN/Weika/models/index_Model_0701"
  
  // val DisperseArr = Array("resp_cd","app_ins_inf","acq_ins_id_cd","mchnt_tp","card_attr","acct_class","app_ins_id_cd","fwd_ins_id_cd","trans_curr_cd","proc_st","ins_pay_mode","up_discount","app_discount","ctrl_rule1","mer_version","app_version","order_type","app_ntf_st","acq_ntf_st","proc_sys","mchnt_back_url","app_back_url","mer_cert_id","mchnt_nm","acq_ins_inf","country_cd","area_cd")
  
     val DisperseArr = Array("resp_cd","app_ins_inf","acq_ins_id_cd","mchnt_tp","card_attr","acct_class","app_ins_id_cd","fwd_ins_id_cd","trans_curr_cd","proc_st","ins_pay_mode","up_discount","app_discount","ctrl_rule1","mer_version","app_version","order_type","app_ntf_st","acq_ntf_st","proc_sys","mer_cert_id","mchnt_nm","acq_ins_inf","country_cd","area_cd")

     
   val ori_sus_Arr = Array("trans_at", "settle_at", "ls_trans_at")
  
   val calc_cols = Array("day_week","hour","is_Night","RMB_bits","is_large_integer","count_89","count_89_ratio","delta_time","delta_at","cur_tot_locs",
      "tot_locs","interval_minutes_1","quant_interval_1","last_money_1","interval_money_1","money_eq_last","money_near_last","cur_tot_amt",
      "cur_tot_cnt","cur_max_amt","cur_min_amt","cur_avg_amt","min_interval_minutes_1","cur_avg_interval","cur_freq_cnt","hist_tot_amt",
      "hist_tot_cnt","hist_max_amt","hist_min_amt","hist_avg_amt","hist_no_trans","timestamp_in_min","min15_tot_amt","min15_tot_cnt","min15_max_amt",
      "min15_min_amt","min15_avg_amt","min15_no_trans","timestamp_in_hour","1hour_tot_amt","1hour_tot_cnt","1hour_max_amt","1hour_min_amt","1hour_avg_amt","1hour_no_trans")
			
}