package PropMap

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.ml.feature.OneHotEncoder

object NfcFeatureOneHotEncoder {

  val columns = Array(
    "acpt_ins_tp",
    "acq_ds_settle_in",
    "acq_ins_tp",
    "addn_pos_inf",
    "block_id",
    "card_attr",
    "card_bin",
    "card_brand",
    "card_class",
    "card_media",
    "card_seq",
    "conn_md",
    "corr_pri_cycle_no",
    "cross_dist_in",
    "cu_trans_st",
    "cups_card_in",
    "cups_sig_card_in",
    "dest_region_cd",
    "disc_in",
    "exp_rsn_cd",
    "fwd_ins_tp",
    "fwd_settle_conv_rt",
    "fwd_settle_curr_cd",
    "iss_ds_settle_in",
    "iss_ins_tp",
    "mchnt_tp",
    "msg_tp",
    "msg_tp_conv",
    "orig_card_media",
    "orig_disc_curr_cd",
    "orig_msg_tp",
    "orig_msg_tp_conv",
    "orig_trans_chnl",
    "orig_trans_id",
    "pos_cond_cd",
    "pos_cond_cd_conv",
    "pos_entry_md_cd",
    "pri_cycle_no",
    "rcv_ins_tp",
    "rcv_settle_conv_rt",
    "rcv_settle_curr_cd",
    "related_card_bin",
    "related_trans_chnl",
    "related_trans_id",
    "resp_cd1",
    "resp_cd2",
    "resp_cd3",
    "resp_cd4",
    "rsn_cd",
    "settle_cycle",
    "settle_tp",
    "sms_dms_conv_in",
    "source_region_cd",
    "sp_mchnt_cd",
    "spec_settle_in",
    "sti_takeout_in",
    "term_tp",
    "tfr_in_in",
    "trans_chnl",
    "trans_curr_cd",
    "trans_fwd_st",
    "trans_id",
    "trans_id_conv",
    "trans_md",
    "trans_media",
    "trans_proc_cd",
    "trans_proc_cd_conv",
    "trans_rcv_st",
    "trans_tp",
    "upd_in")

  val splitColumns = Array(
    "acpt_ins_id_cd_bk",
    "acct_ins_id_cd_bk",
    "acct_ins_id_cd_rg",
    "acpt_ins_id_cd_rg",
    "acq_ins_id_cd_bk",
    "acq_ins_id_cd_rg",
    "fwd_ins_id_cd_bk",
    "fwd_ins_id_cd_rg",
    "iss_ins_id_cd_bk",
    "iss_ins_id_cd_rg",
    "rcv_ins_id_cd_bk",
    "rcv_ins_id_cd_rg",
    "related_ins_id_cd_bk",
    "related_ins_id_cd_rg",
    "settle_fwd_ins_id_cd_bk",
    "settle_fwd_ins_id_cd_rg",
    "settle_rcv_ins_id_cd_bk",
    "settle_rcv_ins_id_cd_rg")

  val allColumns = columns ++ splitColumns

  def getPipeline(): ArrayBuffer[PipelineStage] = {
    val pipelineStages = new ArrayBuffer[PipelineStage]

    for (col <- allColumns) {
      pipelineStages += new StringIndexer()
        .setInputCol(col + "_filled")
        .setOutputCol(col + "_index")
        .setHandleInvalid("skip")
      pipelineStages += new OneHotEncoder()
        .setInputCol(col + "_index")
        .setOutputCol(col + "_vertex")

    }

    //    for (col <- categoryColumns) {
    //      pipelineStages += new VectorAssembler().setInputCols(Array(col)).setOutputCol("vec_" + col)
    //
    //    }

    pipelineStages
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);

    val udf_replaceEmpty = udf[String, String] { xstr =>
      if (xstr.isEmpty())
        "NANs"
      else
        xstr
    }

    val conf = new SparkConf().setAppName("one hot  encode test")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    var transdata = hc.sql(s"select settle_tp,settle_cycle,block_id,trans_fwd_st,trans_rcv_st,sms_dms_conv_in,cross_dist_in,tfr_in_in,trans_md,source_region_cd,dest_region_cd,cups_card_in,cups_sig_card_in,card_class,card_attr,acq_ins_tp,fwd_ins_tp,rcv_ins_tp,iss_ins_tp,acpt_ins_tp,resp_cd1,resp_cd2,resp_cd3,resp_cd4,cu_trans_st,sti_takeout_in,trans_id,trans_tp,trans_chnl,card_media,card_brand,trans_id_conv,trans_curr_cd,conn_md,msg_tp,msg_tp_conv,card_bin,related_card_bin,trans_proc_cd,trans_proc_cd_conv,mchnt_tp,pos_entry_md_cd,card_seq,pos_cond_cd,pos_cond_cd_conv,term_tp,rsn_cd,addn_pos_inf,orig_msg_tp,orig_msg_tp_conv,related_trans_id,related_trans_chnl,orig_trans_id,orig_trans_chnl,orig_card_media,spec_settle_in,iss_ds_settle_in,acq_ds_settle_in,upd_in,exp_rsn_cd,pri_cycle_no,corr_pri_cycle_no,disc_in,orig_disc_curr_cd,fwd_settle_conv_rt,rcv_settle_conv_rt,fwd_settle_curr_cd,rcv_settle_curr_cd,sp_mchnt_cd,trans_media, " + //mchnt_cd,
      s"substring(acq_ins_id_cd,1,4) as acq_ins_id_cd_BK, substring(acq_ins_id_cd,5,4) as acq_ins_id_cd_RG, " +
      s"substring(fwd_ins_id_cd,1,4) as fwd_ins_id_cd_BK, substring(fwd_ins_id_cd,5,4) as fwd_ins_id_cd_RG, " +
      s"substring(rcv_ins_id_cd,1,4) as rcv_ins_id_cd_BK, substring(rcv_ins_id_cd,5,4) as rcv_ins_id_cd_RG, " +
      s"substring(iss_ins_id_cd,1,4) as iss_ins_id_cd_BK, substring(iss_ins_id_cd,5,4) as iss_ins_id_cd_RG, " +
      s"substring(related_ins_id_cd,1,4) as related_ins_id_cd_BK, substring(related_ins_id_cd,5,4) as related_ins_id_cd_RG, " +
      s"substring(acpt_ins_id_cd,1,4) acpt_ins_id_cd_BK, substring(acpt_ins_id_cd,5,4) as acpt_ins_id_cd_RG, " +
      s"substring(settle_fwd_ins_id_cd,1,4) as settle_fwd_ins_id_cd_BK, substring(settle_fwd_ins_id_cd,5,4) as settle_fwd_ins_id_cd_RG, " +
      s"substring(settle_rcv_ins_id_cd,1,4) as settle_rcv_ins_id_cd_BK, substring(settle_rcv_ins_id_cd,5,4) as settle_rcv_ins_id_cd_RG, " +
      s"substring(acct_ins_id_cd,1,4) as acct_ins_id_cd_BK, substring(acct_ins_id_cd,5,4) as acct_ins_id_cd_RG " +
      s"from tbl_common_his_trans where pdate=20160701 and substring(acpt_ins_id_cd,5,4)=3940 ").cache  //.repartition(500).cache()

    println("transdata.count is: " + transdata.count())

    for (oldcol <- allColumns) {
      val newcol = oldcol + "_filled"
      transdata = transdata.withColumn(newcol, udf_replaceEmpty(transdata(oldcol)))
    }
    println("NANs filled done.")


    transdata.select("acpt_ins_tp").distinct().show()

    val pipeline = new Pipeline()
    pipeline.setStages(getPipeline().toArray)

    println("start fitting data!")
    val pipelineModel = pipeline.fit(transdata)
    
    println("start transform data!")
    val result = pipelineModel.transform(transdata)
    println("transform done!")
    result.show()

  }
}
