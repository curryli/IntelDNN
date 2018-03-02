package IntelUtil

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
import scala.collection.mutable.HashMap		
 


object constUtil { 
  def main(args: Array[String]) { 
  }
		
//new
  val usedArr = Array("pri_acct_no_conv","tfr_dt_tm","trans_at","total_disc_at","settle_tp","settle_cycle","block_id","trans_fwd_st","trans_rcv_st","sms_dms_conv_in","cross_dist_in","tfr_in_in","trans_md","source_region_cd","dest_region_cd","cups_card_in","cups_sig_card_in","card_class","card_attr","acq_ins_tp","fwd_ins_tp","rcv_ins_tp","iss_ins_tp","acpt_ins_tp","resp_cd1","resp_cd2","resp_cd3","resp_cd4","cu_trans_st","sti_takeout_in","trans_id","trans_tp","trans_chnl","card_media","trans_id_conv","trans_curr_cd","conn_md","msg_tp","msg_tp_conv","trans_proc_cd","trans_proc_cd_conv","mchnt_tp","pos_entry_md_cd","pos_cond_cd","pos_cond_cd_conv","term_tp","rsn_cd","addn_pos_inf","iss_ds_settle_in","acq_ds_settle_in","upd_in","pri_cycle_no","disc_in","fwd_settle_conv_rt","rcv_settle_conv_rt","fwd_settle_curr_cd","rcv_settle_curr_cd","acq_ins_id_cd_BK","acq_ins_id_cd_RG","fwd_ins_id_cd_BK","fwd_ins_id_cd_RG","rcv_ins_id_cd_BK","rcv_ins_id_cd_RG","iss_ins_id_cd_BK","iss_ins_id_cd_RG","acpt_ins_id_cd_BK","acpt_ins_id_cd_RG","settle_fwd_ins_id_cd_BK","settle_fwd_ins_id_cd_RG","settle_rcv_ins_id_cd_BK","settle_rcv_ins_id_cd_RG", "auth_id_resp_cd","rcv_settle_at","mchnt_cd","term_id") 
  
  //old        
 //val usedArr = Array("pri_acct_no_conv","tfr_dt_tm","trans_at","total_disc_at","settle_tp","settle_cycle","block_id","trans_fwd_st","trans_rcv_st","sms_dms_conv_in","cross_dist_in","tfr_in_in","trans_md","source_region_cd","dest_region_cd","cups_card_in","cups_sig_card_in","card_class","card_attr","acq_ins_tp","fwd_ins_tp","rcv_ins_tp","iss_ins_tp","acpt_ins_tp","resp_cd1","resp_cd2","resp_cd3","resp_cd4","cu_trans_st","sti_takeout_in","trans_id","trans_tp","trans_chnl","card_media","trans_id_conv","trans_curr_cd","conn_md","msg_tp","msg_tp_conv","trans_proc_cd","trans_proc_cd_conv","mchnt_tp","pos_entry_md_cd","pos_cond_cd","pos_cond_cd_conv","term_tp","rsn_cd","addn_pos_inf","iss_ds_settle_in","acq_ds_settle_in","upd_in","pri_cycle_no","disc_in","fwd_settle_conv_rt","rcv_settle_conv_rt","fwd_settle_curr_cd","rcv_settle_curr_cd","acq_ins_id_cd_BK","acq_ins_id_cd_RG","fwd_ins_id_cd_BK","fwd_ins_id_cd_RG","rcv_ins_id_cd_BK","rcv_ins_id_cd_RG","iss_ins_id_cd_BK","iss_ins_id_cd_RG","acpt_ins_id_cd_BK","acpt_ins_id_cd_RG","settle_fwd_ins_id_cd_BK","settle_fwd_ins_id_cd_RG","settle_rcv_ins_id_cd_BK","settle_rcv_ins_id_cd_RG") 

  val DisperseArr = Array("settle_tp","settle_cycle","block_id","trans_fwd_st","trans_rcv_st","sms_dms_conv_in","cross_dist_in","tfr_in_in","trans_md","source_region_cd","dest_region_cd","cups_card_in","cups_sig_card_in","card_class","card_attr","acq_ins_tp","fwd_ins_tp","rcv_ins_tp","iss_ins_tp","acpt_ins_tp","resp_cd1","resp_cd2","resp_cd3","resp_cd4","cu_trans_st","sti_takeout_in","trans_id","trans_tp","trans_chnl","card_media","trans_id_conv","trans_curr_cd","conn_md","msg_tp","msg_tp_conv","trans_proc_cd","trans_proc_cd_conv","mchnt_tp","pos_entry_md_cd","pos_cond_cd","pos_cond_cd_conv","term_tp","rsn_cd","addn_pos_inf","iss_ds_settle_in","acq_ds_settle_in","upd_in","pri_cycle_no","disc_in","fwd_settle_conv_rt","rcv_settle_conv_rt","fwd_settle_curr_cd","rcv_settle_curr_cd","acq_ins_id_cd_BK","acq_ins_id_cd_RG","fwd_ins_id_cd_BK","fwd_ins_id_cd_RG","rcv_ins_id_cd_BK","rcv_ins_id_cd_RG","iss_ins_id_cd_BK","iss_ins_id_cd_RG","acpt_ins_id_cd_BK","acpt_ins_id_cd_RG","settle_fwd_ins_id_cd_BK","settle_fwd_ins_id_cd_RG","settle_rcv_ins_id_cd_BK","settle_rcv_ins_id_cd_RG" ) 
  
  //new
  val Labeled_All_Arr = usedArr.:+("label")
  
  //old
  val labeledArr = Array("pri_acct_no_conv","tfr_dt_tm","day_week","hour","trans_at","total_disc_at","settle_tp","settle_cycle","block_id","trans_fwd_st","trans_rcv_st","sms_dms_conv_in","cross_dist_in","tfr_in_in","trans_md","source_region_cd","dest_region_cd","cups_card_in","cups_sig_card_in","card_class","card_attr","acq_ins_tp","fwd_ins_tp","rcv_ins_tp","iss_ins_tp","acpt_ins_tp","resp_cd1","resp_cd2","resp_cd3","resp_cd4","cu_trans_st","sti_takeout_in","trans_id","trans_tp","trans_chnl","card_media","trans_id_conv","trans_curr_cd","conn_md","msg_tp","msg_tp_conv","trans_proc_cd","trans_proc_cd_conv","mchnt_tp","pos_entry_md_cd","pos_cond_cd","pos_cond_cd_conv","term_tp","rsn_cd","addn_pos_inf","iss_ds_settle_in","acq_ds_settle_in","upd_in","pri_cycle_no","disc_in","fwd_settle_conv_rt","rcv_settle_conv_rt","fwd_settle_curr_cd","rcv_settle_curr_cd","acq_ins_id_cd_BK","acq_ins_id_cd_RG","fwd_ins_id_cd_BK","fwd_ins_id_cd_RG","rcv_ins_id_cd_BK","rcv_ins_id_cd_RG","iss_ins_id_cd_BK","iss_ins_id_cd_RG","acpt_ins_id_cd_BK","acpt_ins_id_cd_RG","settle_fwd_ins_id_cd_BK","settle_fwd_ins_id_cd_RG","settle_rcv_ins_id_cd_BK","settle_rcv_ins_id_cd_RG", "label") 

  //val delArr = Array("pri_acct_no_conv","tfr_dt_tm","day_week","hour","trans_at","total_disc_at","settle_tp","settle_cycle","block_id","trans_fwd_st","sms_dms_conv_in","cross_dist_in","tfr_in_in","trans_md","source_region_cd","dest_region_cd","cups_card_in","cups_sig_card_in","card_class","card_attr","acq_ins_tp","fwd_ins_tp","rcv_ins_tp","iss_ins_tp","acpt_ins_tp","resp_cd1","resp_cd3","cu_trans_st","sti_takeout_in","trans_id","trans_tp","trans_chnl","card_media","trans_curr_cd","conn_md","msg_tp","msg_tp_conv","trans_proc_cd","trans_proc_cd_conv","mchnt_tp","pos_entry_md_cd","pos_cond_cd","pos_cond_cd_conv","term_tp","rsn_cd","addn_pos_inf","iss_ds_settle_in","acq_ds_settle_in","upd_in","pri_cycle_no","disc_in","fwd_settle_conv_rt","fwd_settle_curr_cd","rcv_settle_curr_cd","acq_ins_id_cd_BK","acq_ins_id_cd_RG","fwd_ins_id_cd_BK","fwd_ins_id_cd_RG","rcv_ins_id_cd_BK","rcv_ins_id_cd_RG","iss_ins_id_cd_BK","iss_ins_id_cd_RG","acpt_ins_id_cd_BK","acpt_ins_id_cd_RG","settle_fwd_ins_id_cd_BK","settle_fwd_ins_id_cd_RG","settle_rcv_ins_id_cd_BK","settle_rcv_ins_id_cd_RG", "label")

    
  
  val FE_head = Array("pri_acct_no_conv","label","cur_cross_dist_cnt","day7_tot_amt","is_MC_changed","money_near_last","1hour_max_amt","tot_locs","auth_id_resp_cd","term_id","min5_highrisk_loc_cnt","1hour_highrisk_MCC_cnt","mcnt_locs","2hour_avg_amt","timestamp_in_min","day30_tot_amt","day3_fcnt_term","day3_max_amt","is_highrisk_MC","hist_max_amt","day_week","day7_fraud_cnt","day7_min_amt","hist_fraud_cnt","RMB","min15_highrisk_MCC_cnt","hist_highrisk_loc_cnt","day30_highrisk_MCC_cnt","day3_tot_amt","min5_tot_cnt","day7_freq_cnt","day3_freq_cnt","day3_min_amt","min5_query_cnt","loc_day30_cnt","tfr_dt_tm","min15_tot_amt","day7_tot_cnt","1hour_failure_cnt","day30_fcnt_term","hist_min_amt","day7_highrisk_MCC_cnt","cur_tot_locs","2hour_failure_cnt","min15_highrisk_loc_cnt","quant_interval_1","cur_highrisk_loc_cnt","day7_no_trans","cur_success_cnt","min15_cross_dist_cnt","cur_query_cnt","money_eq_last","hist_failure_cnt","min15_query_cnt","total_disc_at","hist_avg_interval","min15_min_amt","interval_minutes_1","last_mone_1","cur_tot_provs","2hour_min_amt","day7_fcnt_mchnt","cur_min_amt","min15_avg_amt","cur_avg_interval","rcv_settle_at","min5_max_amt","hist_highrisk_MCC_cnt","is_loc_changed","1hour_tot_amt","hist_query_cnt","day3_tot_cnt","1hour_highrisk_loc_cnt","day3_highrisk_loc_cnt","tot_provs","day30_no_trans","2hour_max_amt","hist_success_cnt","cur_tot_amt","day30_fcnt_mchnt","interval_money_1","hist_cross_dist_cnt","mcnt_provs","min15_success_cnt","min15_failure_cnt","1hour_no_trans","trans_at","hour","day7_avg_interval","day30_success_cnt","min5_min_amt","day7_max_amt","day30_fraud_cnt","is_bigRMB_1000","day3_success_cnt","cur_failure_cnt","is_highrisk_MCC","cur_highrisk_MCC_cnt","day30_avg_interval","day3_cross_dist_cnt","cardholder_fail","day3_fraud_cnt","is_bigRMB_500","min5_failure_cnt","is_PW_need","day3_failure_cnt","is_lowrisk_MCC","is_frequent_provs","2hour_highrisk_MCC_cnt","day7_fcnt_term","RMB_bits","date","2hour_tot_amt","2hour_success_cnt","hist_avg_amt","day30_avg_amt","cur_max_amt","day30_min_amt","2hour_highrisk_loc_cnt","day30_highrisk_loc_cnt","day7_avg_amt","day7_failure_cnt","min15_tot_cnt","is_freq_loc_highrisk","loc_day7_cnt","min5_success_cnt","1hour_min_amt","loc_day3_cnt","min5_cross_dist_cnt","day30_cross_dist_cnt","day7_query_cnt","is_spec_airc","timestamp_in_hour","is_success","min5_highrisk_MCC_cnt","day30_freq_cnt","cur_tot_cnt","2hour_query_cnt","min5_tot_amt","day7_cross_dist_cnt","1hour_success_cnt","is_norm_rate","day30_tot_cnt","1hour_query_cnt","hist_freq_cnt","is_frequent_locs","2hour_tot_cnt","min15_no_trans","no_auth_id_resp_cd","day3_highrisk_MCC_cnt","1hour_tot_cnt","is_highrisk_loc","day30_failure_cnt","hist_no_trans","2hour_no_trans","is_Night","day3_no_trans","day3_query_cnt","day7_highrisk_loc_cnt","min15_max_amt","cur_freq_cnt","mchnt_cd","is_large_integer","hist_tot_cnt","day3_avg_amt","min5_avg_amt","min5_no_trans","cur_avg_amt","2hour_cross_dist_cnt","hist_tot_amt","day3_fcnt_mchnt","count_89","day30_max_amt","1hour_avg_amt","day30_query_cnt","day3_avg_interval","day7_success_cnt","1hour_cross_dist_cnt","settle_tp_idx","settle_cycle_idx","block_id_idx","trans_fwd_st_idx","trans_rcv_st_idx","sms_dms_conv_in_idx","cross_dist_in_idx","tfr_in_in_idx","trans_md_idx","source_region_cd_idx","dest_region_cd_idx","cups_card_in_idx","cups_sig_card_in_idx","card_class_idx","card_attr_idx","acq_ins_tp_idx","fwd_ins_tp_idx","rcv_ins_tp_idx","iss_ins_tp_idx","acpt_ins_tp_idx","resp_cd1_idx","resp_cd2_idx","resp_cd3_idx","resp_cd4_idx","cu_trans_st_idx","sti_takeout_in_idx","trans_id_idx","trans_tp_idx","trans_chnl_idx","card_media_idx","trans_id_conv_idx","trans_curr_cd_idx","conn_md_idx","msg_tp_idx","msg_tp_conv_idx","trans_proc_cd_idx","trans_proc_cd_conv_idx","mchnt_tp_idx","pos_entry_md_cd_idx","pos_cond_cd_idx","pos_cond_cd_conv_idx","term_tp_idx","rsn_cd_idx","addn_pos_inf_idx","iss_ds_settle_in_idx","acq_ds_settle_in_idx","upd_in_idx","pri_cycle_no_idx","disc_in_idx","fwd_settle_conv_rt_idx","rcv_settle_conv_rt_idx","fwd_settle_curr_cd_idx","rcv_settle_curr_cd_idx","acq_ins_id_cd_BK_idx","acq_ins_id_cd_RG_idx","fwd_ins_id_cd_BK_idx","fwd_ins_id_cd_RG_idx","rcv_ins_id_cd_BK_idx","rcv_ins_id_cd_RG_idx","iss_ins_id_cd_BK_idx","iss_ins_id_cd_RG_idx","acpt_ins_id_cd_BK_idx","acpt_ins_id_cd_RG_idx","settle_fwd_ins_id_cd_BK_idx","settle_fwd_ins_id_cd_RG_idx","settle_rcv_ins_id_cd_BK_idx","settle_rcv_ins_id_cd_RG_idx") 

  
  
  var schema_251 = new StructType().add("pri_key",StringType,true).add("log_cd",StringType,true).add("settle_tp",StringType,true)
			schema_251 = schema_251.add("settle_cycle",StringType,true).add("block_id",StringType,true).add("orig_key",StringType,true).add("related_key",StringType,true)
			schema_251 = schema_251.add("trans_fwd_st",StringType,true).add("trans_rcv_st",StringType,true).add("sms_dms_conv_in",StringType,true).add("fee_in",StringType,true).add("cross_dist_in",StringType,true).add("orig_acpt_sdms_in",StringType,true).add("tfr_in_in",StringType,true).add("trans_md",StringType,true)
			schema_251 = schema_251.add("source_region_cd",StringType,true).add("dest_region_cd",StringType,true).add("cups_card_in",StringType,true).add("cups_sig_card_in",StringType,true).add("card_class",StringType,true).add("card_attr",StringType,true).add("sti_in",StringType,true).add("trans_proc_in",StringType,true)
			schema_251 = schema_251.add("acq_ins_id_cd",StringType,true).add("acq_ins_tp",StringType,true).add("fwd_ins_id_cd",StringType,true).add("fwd_ins_tp",StringType,true).add("rcv_ins_id_cd",StringType,true).add("rcv_ins_tp",StringType,true).add("iss_ins_id_cd",StringType,true).add("iss_ins_tp",StringType,true)
			schema_251 = schema_251.add("related_ins_id_cd",StringType,true).add("related_ins_tp",StringType,true).add("acpt_ins_id_cd",StringType,true).add("acpt_ins_tp",StringType,true)
			schema_251 = schema_251.add("pri_acct_no",StringType,true).add("pri_acct_no_conv",StringType,true).add("sys_tra_no",StringType,true).add("sys_tra_no_conv",StringType,true).add("sw_sys_tra_no",StringType,true).add("auth_dt",StringType,true).add("auth_id_resp_cd",StringType,true).add("resp_cd1",StringType,true).add("resp_cd2",StringType,true).add("resp_cd3",StringType,true).add("resp_cd4",StringType,true).add("cu_trans_st",StringType,true)
			schema_251 = schema_251.add("sti_takeout_in",StringType,true).add("trans_id",StringType,true).add("trans_tp",StringType,true).add("trans_chnl",StringType,true)
			schema_251 = schema_251.add("card_media",StringType,true).add("card_media_proc_md",StringType,true).add("card_brand",StringType,true).add("expire_seg",StringType,true)
			schema_251 = schema_251.add("trans_id_conv",StringType,true).add("settle_dt",StringType,true).add("settle_mon",StringType,true).add("settle_d",StringType,true)
			schema_251 = schema_251.add("orig_settle_dt",StringType,true).add("settle_fwd_ins_id_cd",StringType,true).add("settle_rcv_ins_id_cd",StringType,true).add("trans_at",StringType,true).add("orig_trans_at",StringType,true).add("trans_conv_rt",StringType,true).add("trans_curr_cd",StringType,true)
			schema_251 = schema_251.add("cdhd_fee_at",StringType,true).add("cdhd_fee_conv_rt",StringType,true).add("cdhd_fee_acct_curr_cd",StringType,true).add("repl_at",StringType,true)
			schema_251 = schema_251.add("exp_snd_chnl",StringType,true).add("confirm_exp_chnl",StringType,true).add("extend_inf",StringType,true).add("conn_md",StringType,true).add("msg_tp",StringType,true).add("msg_tp_conv",StringType,true).add("card_bin",StringType,true).add("related_card_bin",StringType,true)
			schema_251 = schema_251.add("trans_proc_cd",StringType,true).add("trans_proc_cd_conv",StringType,true).add("tfr_dt_tm",StringType,true).add("loc_trans_tm",StringType,true)
			schema_251 = schema_251.add("loc_trans_dt",StringType,true).add("conv_dt",StringType,true).add("mchnt_tp",StringType,true).add("pos_entry_md_cd",StringType,true).add("card_seq",StringType,true).add("pos_cond_cd",StringType,true).add("pos_cond_cd_conv",StringType,true).add("retri_ref_no",StringType,true)
			schema_251 = schema_251.add("term_id",StringType,true).add("term_tp",StringType,true).add("mchnt_cd",StringType,true).add("card_accptr_nm_addr",StringType,true)
			schema_251 = schema_251.add("ic_data",StringType,true).add("rsn_cd",StringType,true).add("addn_pos_inf",StringType,true).add("orig_msg_tp",StringType,true).add("orig_msg_tp_conv",StringType,true).add("orig_sys_tra_no",StringType,true).add("orig_sys_tra_no_conv",StringType,true)
			schema_251 = schema_251.add("orig_tfr_dt_tm",StringType,true).add("related_trans_id",StringType,true).add("related_trans_chnl",StringType,true)
			schema_251 = schema_251.add("orig_trans_id",StringType,true).add("orig_trans_id_conv",StringType,true).add("orig_trans_chnl",StringType,true).add("orig_card_media",StringType,true).add("orig_card_media_proc_md",StringType,true).add("tfr_in_acct_no",StringType,true).add("tfr_out_acct_no",StringType,true).add("cups_resv",StringType,true)
			schema_251 = schema_251.add("ic_flds",StringType,true).add("cups_def_fld",StringType,true).add("spec_settle_in",StringType,true).add("settle_trans_id",StringType,true)
			schema_251 = schema_251.add("spec_mcc_in",StringType,true).add("iss_ds_settle_in",StringType,true).add("acq_ds_settle_in",StringType,true).add("settle_bmp",StringType,true)
			schema_251 = schema_251.add("upd_in",StringType,true).add("exp_rsn_cd",StringType,true).add("to_ts",StringType,true).add("resnd_num",StringType,true)
			schema_251 = schema_251.add("pri_cycle_no",StringType,true).add("alt_cycle_no",StringType,true).add("corr_pri_cycle_no",StringType,true).add("corr_alt_cycle_no",StringType,true)
			schema_251 = schema_251.add("disc_in",StringType,true).add("vfy_rslt",StringType,true).add("vfy_fee_cd",StringType,true).add("orig_disc_in",StringType,true).add("orig_disc_curr_cd",StringType,true).add("fwd_settle_at",StringType,true).add("rcv_settle_at",StringType,true).add("fwd_settle_conv_rt",StringType,true)
			schema_251 = schema_251.add("rcv_settle_conv_rt",StringType,true).add("fwd_settle_curr_cd",StringType,true).add("rcv_settle_curr_cd",StringType,true).add("disc_cd",StringType,true).add("allot_cd",StringType,true).add("total_disc_at",StringType,true).add("fwd_orig_settle_at",StringType,true).add("rcv_orig_settle_at",StringType,true)
			schema_251 = schema_251.add("vfy_fee_at",StringType,true).add("sp_mchnt_cd",StringType,true).add("acct_ins_id_cd",StringType,true).add("iss_ins_id_cd1",StringType,true)
			schema_251 = schema_251.add("iss_ins_id_cd2",StringType,true).add("iss_ins_id_cd3",StringType,true).add("iss_ins_id_cd4",StringType,true).add("mchnt_ins_id_cd1",StringType,true).add("mchnt_ins_id_cd2",StringType,true).add("mchnt_ins_id_cd3",StringType,true).add("mchnt_ins_id_cd4",StringType,true).add("term_ins_id_cd1",StringType,true).add("term_ins_id_cd2",StringType,true).add("term_ins_id_cd3",StringType,true).add("term_ins_id_cd4",StringType,true)
			schema_251 = schema_251.add("term_ins_id_cd5",StringType,true).add("acpt_cret_disc_at",StringType,true).add("acpt_debt_disc_at",StringType,true)
			schema_251 = schema_251.add("iss1_cret_disc_at",StringType,true).add("iss1_debt_disc_at",StringType,true).add("iss2_cret_disc_at",StringType,true).add("iss2_debt_disc_at",StringType,true).add("iss3_cret_disc_at",StringType,true).add("iss3_debt_disc_at",StringType,true).add("iss4_cret_disc_at",StringType,true).add("iss4_debt_disc_at",StringType,true).add("mchnt1_cret_disc_at",StringType,true)
			schema_251 = schema_251.add("mchnt1_debt_disc_at",StringType,true).add("mchnt2_cret_disc_at",StringType,true).add("mchnt2_debt_disc_at",StringType,true).add("mchnt3_cret_disc_at",StringType,true).add("mchnt3_debt_disc_at",StringType,true).add("mchnt4_cret_disc_at",StringType,true).add("mchnt4_debt_disc_at",StringType,true).add("term1_cret_disc_at",StringType,true).add("term1_debt_disc_at",StringType,true).add("term2_cret_disc_at",StringType,true).add("term2_debt_disc_at",StringType,true).add("term3_cret_disc_at",StringType,true)
			schema_251 = schema_251.add("term3_debt_disc_at",StringType,true).add("term4_cret_disc_at",StringType,true).add("term4_debt_disc_at",StringType,true)
			schema_251 = schema_251.add("term5_cret_disc_at",StringType,true).add("term5_debt_disc_at",StringType,true).add("pay_in",StringType,true).add("exp_id",StringType,true).add("vou_in",StringType,true).add("orig_log_cd",StringType,true).add("related_log_cd",StringType,true).add("rec_upd_ts",StringType,true).add("rec_crt_ts",StringType,true).add("ext_mdc_key",StringType,true).add("ext_conn_in",StringType,true).add("ext_cross_region_in",StringType,true)
			schema_251 = schema_251.add("ext_auth_dt",StringType,true).add("ext_trans_st",StringType,true).add("ext_trans_attr",StringType,true).add("ext_trans_at",StringType,true)
			schema_251 = schema_251.add("ext_transmsn_dt_tm",StringType,true).add("ext_orig_transmsn_dt_tm",StringType,true).add("ext_card_accptr_nm_loc",StringType,true).add("ext_card_bin",StringType,true).add("ext_card_bin_id",StringType,true).add("ext_src_busi_key",StringType,true).add("ext_card_brand_cd",StringType,true).add("ext_card_attr_cd",StringType,true).add("ext_card_prod_cd",StringType,true).add("ext_card_media_proc_md",StringType,true).add("ext_status_code",StringType,true).add("ext_cups_resv_1_5",StringType,true).add("ext_cups_resv_6_9",StringType,true)
			schema_251 = schema_251.add("ext_cups_resv_10_49",StringType,true).add("ext_cups_resv_50_102",StringType,true).add("ext_busi_dt",StringType,true)
			schema_251 = schema_251.add("ext_extend_region_in",StringType,true).add("ext_eci_flag",StringType,true).add("ext_extend_inf_4_12",StringType,true).add("ext_extend_inf_13",StringType,true).add("ext_extend_inf_14_19",StringType,true).add("ext_extend_inf_20",StringType,true).add("ext_extend_inf_21",StringType,true).add("ext_extend_inf_22",StringType,true).add("ext_extend_inf_23_27",StringType,true).add("ext_extend_inf_28",StringType,true).add("ext_token_no",StringType,true).add("ext_extend_inf_51_54",StringType,true).add("ext_extend_inf_55_56",StringType,true).add("ext_vfy_fee_cd_1_35",StringType,true).add("ext_vfy_fee_cd_36_53",StringType,true).add("ext_vfy_fee_cd_54_72",StringType,true)
			schema_251 = schema_251.add("ext_vfy_fee_cd_73_74",StringType,true).add("ext_vfy_fee_cd_75_76",StringType,true).add("ext_vfy_fee_cd_77_87",StringType,true).add("ext_total_card_prod",StringType,true).add("ext_ma_fwd_ins_id_cd",StringType,true).add("ext_ma_industry_ins_id_cd",StringType,true).add("ext_ma_ext_trans_tp",StringType,true).add("ext_acq_ins_id_cd",StringType,true).add("ext_hce_prod_nm",StringType,true).add("ext_mchnt_tp",StringType,true).add("ext_cups_disc_at",StringType,true).add("ext_iss_disc_at",StringType,true).add("ext_spec_disc_tp",StringType,true).add("ext_spec_disc_lvl",StringType,true).add("ext_hce_prod_in",StringType,true).add("ext_touch_tp",StringType,true).add("ext_carrier_tp",StringType,true)
			//schema_251 = schema_251.add("ext_nopwd_petty_in",StringType,true).add("ext_log_id",StringType,true).add("ext_white_mchnt_in",StringType,true).add("ext_up_cloud_app_in",StringType,true).add("ext_pri_acct_no",StringType,true)
			//schema_251 = schema_251.add("trans_industry_app_inf",StringType,true).add("ext_transfer_in_id_inf",StringType,true).add("ext_transfer_out_id_inf",StringType,true).add("ext_realtime_transfer_in",StringType,true).add("ext_machine_seq",StringType,true).add("ext_rand_factor",StringType,true).add("ext_machine_seq_mac",StringType,true).add("ext_pre_auth_valid_date",StringType,true).add("ext_pre_auth_invalid_date",StringType,true).add("ext_pre_auth_type",StringType,true).add("ext_pre_auth_date",StringType,true).add("sec_risk_ass_inf",StringType,true).add("ext_qr_in",StringType,true).add("ext_payment_no",StringType,true).add("pdate",StringType,true)
		  
  
		
			var schema_labeled = new StructType().add("pri_acct_no_conv",StringType,true).add("tfr_dt_tm",DoubleType,true).add("day_week",DoubleType,true).add("hour",DoubleType,true).add("trans_at",DoubleType,true).add("total_disc_at",DoubleType,true).add("settle_tp",DoubleType,true).add("settle_cycle",DoubleType,true).add("block_id",DoubleType,true).add("trans_fwd_st",DoubleType,true).add("trans_rcv_st",DoubleType,true).add("sms_dms_conv_in",DoubleType,true).add("cross_dist_in",DoubleType,true).add("tfr_in_in",DoubleType,true).add("trans_md",DoubleType,true).add("source_region_cd",DoubleType,true).add("dest_region_cd",DoubleType,true).add("cups_card_in",DoubleType,true).add("cups_sig_card_in",DoubleType,true).add("card_class",DoubleType,true).add("card_attr",DoubleType,true).add("acq_ins_tp",DoubleType,true).add("fwd_ins_tp",DoubleType,true).add("rcv_ins_tp",DoubleType,true).add("iss_ins_tp",DoubleType,true).add("acpt_ins_tp",DoubleType,true).add("resp_cd1",DoubleType,true).add("resp_cd2",DoubleType,true).add("resp_cd3",DoubleType,true).add("resp_cd4",DoubleType,true).add("cu_trans_st",DoubleType,true).add("sti_takeout_in",DoubleType,true).add("trans_id",DoubleType,true).add("trans_tp",DoubleType,true).add("trans_chnl",DoubleType,true).add("card_media",DoubleType,true).add("trans_id_conv",DoubleType,true).add("trans_curr_cd",DoubleType,true).add("conn_md",DoubleType,true).add("msg_tp",DoubleType,true).add("msg_tp_conv",DoubleType,true).add("trans_proc_cd",DoubleType,true).add("trans_proc_cd_conv",DoubleType,true).add("mchnt_tp",DoubleType,true).add("pos_entry_md_cd",DoubleType,true).add("pos_cond_cd",DoubleType,true).add("pos_cond_cd_conv",DoubleType,true).add("term_tp",DoubleType,true).add("rsn_cd",DoubleType,true).add("addn_pos_inf",DoubleType,true).add("iss_ds_settle_in",DoubleType,true).add("acq_ds_settle_in",DoubleType,true).add("upd_in",DoubleType,true).add("pri_cycle_no",DoubleType,true).add("disc_in",DoubleType,true).add("fwd_settle_conv_rt",DoubleType,true).add("rcv_settle_conv_rt",DoubleType,true).add("fwd_settle_curr_cd",DoubleType,true).add("rcv_settle_curr_cd",DoubleType,true).add("acq_ins_id_cd_BK",DoubleType,true).add("acq_ins_id_cd_RG",DoubleType,true).add("fwd_ins_id_cd_BK",DoubleType,true).add("fwd_ins_id_cd_RG",DoubleType,true).add("rcv_ins_id_cd_BK",DoubleType,true).add("rcv_ins_id_cd_RG",DoubleType,true).add("iss_ins_id_cd_BK",DoubleType,true).add("iss_ins_id_cd_RG",DoubleType,true).add("acpt_ins_id_cd_BK",DoubleType,true).add("acpt_ins_id_cd_RG",DoubleType,true).add("settle_fwd_ins_id_cd_BK",DoubleType,true).add("settle_fwd_ins_id_cd_RG",DoubleType,true).add("settle_rcv_ins_id_cd_BK",DoubleType,true).add("settle_rcv_ins_id_cd_RG",DoubleType,true).add("label",DoubleType,true) 

    
			 
//		var date_to_num_Map = HashMap(
//   	  "20170701"->1,"20170702"->2,"20170703"->3,"20170704"->4,"20170705"->5,"20170706"->6,"20170707"->7,"20170708"->8,"20170709"->9,"20170710"->10)
//			
//   var dateMap = HashMap(
//  		1->"20170701",2->"20170702",3->"20170703",4->"20170704",5->"20170705",6->"20170706",7->"20170707",8->"20170708",9->"20170709",10->"20170710")
			
	
			
			
			
	var Risk_mchnt_cd_List = List("104100557221151","104110548991091","104110548991184","104110548991187","104110548991188","104310048995336","105290047225485","105290048160245","105290073991062","303310048990898","303310048990990","305330148160001","","308310054113653","308310054113654","308310054113655","461430148160001","802110048120001","802310048992563","802310049000529","802310049000534","802310049000535","802310049000538","802310049000539","802310049000540","802310049000541","802310049000542","802310049000543","802310049000546","802310049000547","802310049000548","802310049000549","802310049000550","802310049000551","802310049000552","802310049000553","802310049000554","802310049000555","802310049000556","802310049000557","802310049000558","802310049000559","802310049000560","802310049000561","802310049000562","802310049000563","802310049000564","802310049000565","802310049000566","802310049000567","802310049000568","802310049000569","802310049000573","802310049000574","802310049000575","802310049000576","802310049000577","802310049000579","802310049000581","802310049000582","802310049000583","802310049000584","802310049000585","802310051720500","802310052000629","898110248990014","898110248990216","898110248990302","898111148990268","898111153990172","898320147220157","898320173990257","898320547220117","104530148163000","104110557221352","104100557221152","104100557221151","104100557221153","303430148160116","443701049000008","443701048990222","443701094980169","105290048140580","802410079930500","802440048990915","898111153990166","898110148999611","320100048991008","898440048120004","898510148990112","898440348160124","898440383981690","802310048990774","802310053110500","898111153990172","100000000017585","100000000017585","104110548991184","104110548991184","104110548991184","104110548991184","104110548991184","104110548991184","104110548991184")
		
	
	var HighRisk_Loc = List("39", "40", "41","42", "43", "44", "64")
	var LowRisk_MCC = List("3999","4111","4899","5309","5960","6012","6051","6300","6513","8011","8043","8062","8099","8211","8220","8398","8888","9311","9399")
	var HighRisk_MCC = List("4214","4511","5065","5094","5211","5251","5310","5331","5533","5712","5722","5732","5932","5947","5993","7221")
		
}