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
		
  val DisperseArr = Array("settle_tp","settle_cycle","block_id","trans_fwd_st","trans_rcv_st","sms_dms_conv_in","cross_dist_in","tfr_in_in","trans_md","source_region_cd","dest_region_cd","cups_card_in","cups_sig_card_in","card_class","card_attr","acq_ins_tp","fwd_ins_tp","rcv_ins_tp","iss_ins_tp","acpt_ins_tp","resp_cd1","resp_cd2","resp_cd3","resp_cd4","cu_trans_st","sti_takeout_in","trans_id","trans_tp","trans_chnl","card_media","card_brand","trans_id_conv","trans_curr_cd","conn_md","msg_tp","msg_tp_conv","trans_proc_cd","trans_proc_cd_conv","mchnt_tp","pos_entry_md_cd","card_seq","pos_cond_cd","pos_cond_cd_conv","term_tp","rsn_cd","addn_pos_inf","orig_msg_tp","orig_msg_tp_conv","related_trans_id","related_trans_chnl","orig_trans_id","orig_trans_chnl","orig_card_media","spec_settle_in","iss_ds_settle_in","acq_ds_settle_in","upd_in","exp_rsn_cd","pri_cycle_no","corr_pri_cycle_no","disc_in","orig_disc_curr_cd","fwd_settle_conv_rt","rcv_settle_conv_rt","fwd_settle_curr_cd","rcv_settle_curr_cd","sp_mchnt_cd","acq_ins_id_cd_BK","acq_ins_id_cd_RG","fwd_ins_id_cd_BK","fwd_ins_id_cd_RG","rcv_ins_id_cd_BK","rcv_ins_id_cd_RG","iss_ins_id_cd_BK","iss_ins_id_cd_RG","related_ins_id_cd_BK","related_ins_id_cd_RG","acpt_ins_id_cd_BK","acpt_ins_id_cd_RG","settle_fwd_ins_id_cd_BK","settle_fwd_ins_id_cd_RG","settle_rcv_ins_id_cd_BK","settle_rcv_ins_id_cd_RG","acct_ins_id_cd_BK","acct_ins_id_cd_RG")    //"card_bin","related_card_bin",

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
		  
	 
		var date_to_num_Map = HashMap(
   	  "20160701"->1,"20160702"->2,"20160703"->3,"20160704"->4,"20160705"->5,"20160706"->6,"20160707"->7,"20160708"->8,"20160709"->9,"20160710"->10,
			"20160711"->11,"20160712"->12,"20160713"->13,"20160714"->14,"20160715"->15,"20160716"->16,"20160717"->17,"20160718"->18,"20160719"->19,"20160720"->20,
			"20160721"->21,"20160722"->22,"20160723"->23,"20160724"->24,"20160725"->25,"20160726"->26,"20160727"->27,"20160728"->28,"20160729"->29,"20160730"->30,
			"20160731"->31,"20160801"->32,"20160802"->33,"20160803"->34,"20160804"->35,"20160805"->36,"20160806"->37,"20160807"->38,"20160808"->39,"20160809"->40,
			"20160810"->41,"20160811"->42,"20160812"->43,"20160813"->44,"20160814"->45,"20160815"->46,"20160816"->47,"20160817"->48,"20160818"->49,"20160819"->50,
			"20160820"->51,"20160821"->52,"20160822"->53,"20160823"->54,"20160824"->55,"20160825"->56,"20160826"->57,"20160827"->58,"20160828"->59,"20160829"->60,
			"20160830"->61,"20160831"->62,"20160901"->63,"20160902"->64,"20160903"->65,"20160904"->66,"20160905"->67,"20160906"->68,"20160907"->69,"20160908"->70,
			"20160909"->71,"20160910"->72,"20160911"->73,"20160912"->74,"20160913"->75,"20160914"->76,"20160915"->77,"20160916"->78,"20160917"->79,"20160918"->80,
			"20160919"->81,"20160920"->82,"20160921"->83,"20160922"->84,"20160923"->85,"20160924"->86,"20160925"->87,"20160926"->88,"20160927"->89,"20160928"->90,
			"20160929"->91,"20160930"->92,"20161001"->93,"20161002"->94,"20161003"->95,"20161004"->96,"20161005"->97,"20161006"->98,"20161007"->99,"20161008"->100,
			"20161009"->101,"20161010"->102,"20161011"->103,"20161012"->104,"20161013"->105,"20161014"->106,"20161015"->107,"20161016"->108,"20161017"->109,"20161018"->110,
			"20161019"->111,"20161020"->112,"20161021"->113,"20161022"->114,"20161023"->115,"20161024"->116,"20161025"->117,"20161026"->118,"20161027"->119,"20161028"->120,
			"20161029"->121,"20161030"->122,"20161031"->123,"20161101"->124,"20161102"->125,"20161103"->126,"20161104"->127,"20161105"->128,"20161106"->129,"20161107"->130,
			"20161108"->131,"20161109"->132,"20161110"->133,"20161111"->134,"20161112"->135,"20161113"->136,"20161114"->137,"20161115"->138,"20161116"->139,"20161117"->140,
			"20161118"->141,"20161119"->142,"20161120"->143,"20161121"->144,"20161122"->145,"20161123"->146,"20161124"->147,"20161125"->148,"20161126"->149,"20161127"->150,
			"20161128"->151,"20161129"->152,"20161130"->153,"20161201"->154,"20161202"->155,"20161203"->156,"20161204"->157,"20161205"->158,"20161206"->159,"20161207"->160,
			"20161208"->161,"20161209"->162,"20161210"->163,"20161211"->164,"20161212"->165,"20161213"->166,"20161214"->167,"20161215"->168,"20161216"->169,"20161217"->170,
			"20161218"->171,"20161219"->172,"20161220"->173,"20161221"->174,"20161222"->175,"20161223"->176,"20161224"->177,"20161225"->178,"20161226"->179,"20161227"->180,
			"20161228"->181,"20161229"->182,"20161230"->183,"20161231"->184)
			
   var dateMap = HashMap(
  		1->"20160701",2->"20160702",3->"20160703",4->"20160704",5->"20160705",6->"20160706",7->"20160707",8->"20160708",9->"20160709",10->"20160710",
			11->"20160711",12->"20160712",13->"20160713",14->"20160714",15->"20160715",16->"20160716",17->"20160717",18->"20160718",19->"20160719",20->"20160720",
			21->"20160721",22->"20160722",23->"20160723",24->"20160724",25->"20160725",26->"20160726",27->"20160727",28->"20160728",29->"20160729",30->"20160730",
			31->"20160731",32->"20160801",33->"20160802",34->"20160803",35->"20160804",36->"20160805",37->"20160806",38->"20160807",39->"20160808",40->"20160809",
			41->"20160810",42->"20160811",43->"20160812",44->"20160813",45->"20160814",46->"20160815",47->"20160816",48->"20160817",49->"20160818",50->"20160819",
			51->"20160820",52->"20160821",53->"20160822",54->"20160823",55->"20160824",56->"20160825",57->"20160826",58->"20160827",59->"20160828",60->"20160829",
			61->"20160830",62->"20160831",63->"20160901",64->"20160902",65->"20160903",66->"20160904",67->"20160905",68->"20160906",69->"20160907",70->"20160908",
			71->"20160909",72->"20160910",73->"20160911",74->"20160912",75->"20160913",76->"20160914",77->"20160915",78->"20160916",79->"20160917",80->"20160918",
			81->"20160919",82->"20160920",83->"20160921",84->"20160922",85->"20160923",86->"20160924",87->"20160925",88->"20160926",89->"20160927",90->"20160928",
			91->"20160929",92->"20160930",93->"20161001",94->"20161002",95->"20161003",96->"20161004",97->"20161005",98->"20161006",99->"20161007",100->"20161008",
			101->"20161009",102->"20161010",103->"20161011",104->"20161012",105->"20161013",106->"20161014",107->"20161015",108->"20161016",109->"20161017",110->"20161018",
			111->"20161019",112->"20161020",113->"20161021",114->"20161022",115->"20161023",116->"20161024",117->"20161025",118->"20161026",119->"20161027",120->"20161028",
			121->"20161029",122->"20161030",123->"20161031",124->"20161101",125->"20161102",126->"20161103",127->"20161104",128->"20161105",129->"20161106",130->"20161107",
			131->"20161108",132->"20161109",133->"20161110",134->"20161111",135->"20161112",136->"20161113",137->"20161114",138->"20161115",139->"20161116",140->"20161117",
			141->"20161118",142->"20161119",143->"20161120",144->"20161121",145->"20161122",146->"20161123",147->"20161124",148->"20161125",149->"20161126",150->"20161127",
			151->"20161128",152->"20161129",153->"20161130",154->"20161201",155->"20161202",156->"20161203",157->"20161204",158->"20161205",159->"20161206",160->"20161207",
			161->"20161208",162->"20161209",163->"20161210",164->"20161211",165->"20161212",166->"20161213",167->"20161214",168->"20161215",169->"20161216",170->"20161217",
			171->"20161218",172->"20161219",173->"20161220",174->"20161221",175->"20161222",176->"20161223",177->"20161224",178->"20161225",179->"20161226",180->"20161227",
			181->"20161228",182->"20161229",183->"20161230",184->"20161231" )
			
			
}