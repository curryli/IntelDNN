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
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.VectorUDT

object vectorization {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);
    
   val conf = new SparkConf().setAppName("compare_2time")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
     
    var transdata = hc.sql(s"select settle_tp,settle_cycle,block_id,trans_fwd_st,trans_rcv_st,sms_dms_conv_in,cross_dist_in,tfr_in_in,trans_md,source_region_cd,dest_region_cd,cups_card_in,cups_sig_card_in,card_class,card_attr,acq_ins_tp,fwd_ins_tp,rcv_ins_tp,iss_ins_tp,acpt_ins_tp,resp_cd1,resp_cd2,resp_cd3,resp_cd4,cu_trans_st,sti_takeout_in,trans_id,trans_tp,trans_chnl,card_media,card_brand,trans_id_conv,trans_curr_cd,conn_md,msg_tp,msg_tp_conv,card_bin,related_card_bin,trans_proc_cd,trans_proc_cd_conv,mchnt_tp,pos_entry_md_cd,card_seq,pos_cond_cd,pos_cond_cd_conv,term_tp,rsn_cd,addn_pos_inf,orig_msg_tp,orig_msg_tp_conv,related_trans_id,related_trans_chnl,orig_trans_id,orig_trans_chnl,orig_card_media,spec_settle_in,iss_ds_settle_in,acq_ds_settle_in,upd_in,exp_rsn_cd,pri_cycle_no,corr_pri_cycle_no,disc_in,orig_disc_curr_cd,fwd_settle_conv_rt,rcv_settle_conv_rt,fwd_settle_curr_cd,rcv_settle_curr_cd,sp_mchnt_cd,trans_media, " +    //mchnt_cd,
      s"substring(acq_ins_id_cd,1,4) as acq_ins_id_cd_BK, substring(acq_ins_id_cd,5,4) as acq_ins_id_cd_RG, " + 
      s"substring(fwd_ins_id_cd,1,4) as fwd_ins_id_cd_BK, substring(fwd_ins_id_cd,5,4) as fwd_ins_id_cd_RG, " + 
      s"substring(rcv_ins_id_cd,1,4) as rcv_ins_id_cd_BK, substring(rcv_ins_id_cd,5,4) as rcv_ins_id_cd_RG, " + 
      s"substring(iss_ins_id_cd,1,4) as iss_ins_id_cd_BK, substring(iss_ins_id_cd,5,4) as iss_ins_id_cd_RG, " + 
      s"substring(related_ins_id_cd,1,4) as related_ins_id_cd_BK, substring(related_ins_id_cd,5,4) as related_ins_id_cd_RG, " + 
      s"substring(acpt_ins_id_cd,1,4) acpt_ins_id_cd_BK, substring(acpt_ins_id_cd,5,4) as acpt_ins_id_cd_RG, " + 
      s"substring(settle_fwd_ins_id_cd,1,4) as settle_fwd_ins_id_cd_BK, substring(settle_fwd_ins_id_cd,5,4) as settle_fwd_ins_id_cd_RG, " + 
      s"substring(settle_rcv_ins_id_cd,1,4) as settle_rcv_ins_id_cd_BK, substring(settle_rcv_ins_id_cd,5,4) as settle_rcv_ins_id_cd_RG, " + 
      s"substring(acct_ins_id_cd,1,4) as acct_ins_id_cd_BK, substring(acct_ins_id_cd,5,4) as acct_ins_id_cd_RG " +
	    s"from tbl_common_his_trans where pdate=20160701") 
  
	    println(transdata.columns.length)
   
     //val DisperseArr = Array("settle_tp","settle_cycle","block_id","trans_fwd_st","trans_rcv_st","sms_dms_conv_in","cross_dist_in","tfr_in_in","trans_md","source_region_cd","dest_region_cd","cups_card_in","cups_sig_card_in","card_class","card_attr","acq_ins_tp","fwd_ins_tp","rcv_ins_tp","iss_ins_tp","acpt_ins_tp","resp_cd1","resp_cd2","resp_cd3","resp_cd4","cu_trans_st","sti_takeout_in","trans_id","trans_tp","trans_chnl","card_media","card_brand","trans_id_conv","trans_curr_cd","conn_md","msg_tp","msg_tp_conv","card_bin","related_card_bin","trans_proc_cd","trans_proc_cd_conv","mchnt_tp","pos_entry_md_cd","card_seq","pos_cond_cd","pos_cond_cd_conv","term_tp","rsn_cd","addn_pos_inf","orig_msg_tp","orig_msg_tp_conv","related_trans_id","related_trans_chnl","orig_trans_id","orig_trans_chnl","orig_card_media","spec_settle_in","iss_ds_settle_in","acq_ds_settle_in","upd_in","exp_rsn_cd","pri_cycle_no","corr_pri_cycle_no","disc_in","orig_disc_curr_cd","fwd_settle_conv_rt","rcv_settle_conv_rt","fwd_settle_curr_cd","rcv_settle_curr_cd","sp_mchnt_cd","trans_media","acq_ins_id_cd_BK","acq_ins_id_cd_RG","fwd_ins_id_cd_BK","fwd_ins_id_cd_RG","rcv_ins_id_cd_BK","rcv_ins_id_cd_RG","iss_ins_id_cd_BK","iss_ins_id_cd_RG","related_ins_id_cd_BK","related_ins_id_cd_RG","acpt_ins_id_cd_BK","acpt_ins_id_cd_RG","settle_fwd_ins_id_cd_BK","settle_fwd_ins_id_cd_RG","settle_rcv_ins_id_cd_BK","settle_rcv_ins_id_cd_RG","acct_ins_id_cd_BK","acct_ins_id_cd_RG")
          
	   val DisperseArr = Array( "source_region_cd", "dest_region_cd", "block_id","trans_curr_cd","mchnt_tp") 
	    
     val udf_replaceEmpty = udf[String, String]{xstr => 
        if(xstr.isEmpty())
          "NANs"
        else
          xstr
      }
   
     var i = 0
     for(oldcol <- DisperseArr){
        var newcol = oldcol + "_filled" 
        println(i)
        i = i + 1 
        transdata = transdata.withColumn(newcol, udf_replaceEmpty(transdata(oldcol)))
        //transdata = transdata.drop(oldcol)
     }   
        
        
      for(oldcol <- DisperseArr){  
        var filledcol = oldcol + "_filled"
        var indexCat = oldcol + "_indexCat"
        var indexer = new StringIndexer().setInputCol(filledcol).setOutputCol(indexCat)
        transdata = indexer.fit(transdata).transform(transdata)
      }
     
     //val IndexeCatArr = Array("settle_tp_indexCat","settle_cycle_indexCat","block_id_indexCat","trans_fwd_st_indexCat","trans_rcv_st_indexCat","sms_dms_conv_in_indexCat","cross_dist_in_indexCat","tfr_in_in_indexCat","trans_md_indexCat","source_region_cd_indexCat","dest_region_cd_indexCat","cups_card_in_indexCat","cups_sig_card_in_indexCat","card_class_indexCat","card_attr_indexCat","acq_ins_tp_indexCat","fwd_ins_tp_indexCat","rcv_ins_tp_indexCat","iss_ins_tp_indexCat","acpt_ins_tp_indexCat","resp_cd1_indexCat","resp_cd2_indexCat","resp_cd3_indexCat","resp_cd4_indexCat","cu_trans_st_indexCat","sti_takeout_in_indexCat","trans_id_indexCat","trans_tp_indexCat","trans_chnl_indexCat","card_media_indexCat","card_brand_indexCat","trans_id_conv_indexCat","trans_curr_cd_indexCat","conn_md_indexCat","msg_tp_indexCat","msg_tp_conv_indexCat","card_bin_indexCat","related_card_bin_indexCat","trans_proc_cd_indexCat","trans_proc_cd_conv_indexCat","mchnt_tp_indexCat","pos_entry_md_cd_indexCat","card_seq_indexCat","pos_cond_cd_indexCat","pos_cond_cd_conv_indexCat","term_tp_indexCat","rsn_cd_indexCat","addn_pos_inf_indexCat","orig_msg_tp_indexCat","orig_msg_tp_conv_indexCat","related_trans_id_indexCat","related_trans_chnl_indexCat","orig_trans_id_indexCat","orig_trans_chnl_indexCat","orig_card_media_indexCat","spec_settle_in_indexCat","iss_ds_settle_in_indexCat","acq_ds_settle_in_indexCat","upd_in_indexCat","exp_rsn_cd_indexCat","pri_cycle_no_indexCat","corr_pri_cycle_no_indexCat","disc_in_indexCat","orig_disc_curr_cd_indexCat","fwd_settle_conv_rt_indexCat","rcv_settle_conv_rt_indexCat","fwd_settle_curr_cd_indexCat","rcv_settle_curr_cd_indexCat","sp_mchnt_cd_indexCat","trans_media_indexCat","acq_ins_id_cd_BK_indexCat","acq_ins_id_cd_RG_indexCat","fwd_ins_id_cd_BK_indexCat","fwd_ins_id_cd_RG_indexCat","rcv_ins_id_cd_BK_indexCat","rcv_ins_id_cd_RG_indexCat","iss_ins_id_cd_BK_indexCat","iss_ins_id_cd_RG_indexCat","related_ins_id_cd_BK_indexCat","related_ins_id_cd_RG_indexCat","acpt_ins_id_cd_BK_indexCat","acpt_ins_id_cd_RG_indexCat","settle_fwd_ins_id_cd_BK_indexCat","settle_fwd_ins_id_cd_RG_indexCat","settle_rcv_ins_id_cd_BK_indexCat","settle_rcv_ins_id_cd_RG_indexCat","acct_ins_id_cd_BK_indexCat","acct_ins_id_cd_RG_indexCat")
     val IndexeCatArr = Array("source_region_cd_indexCat", "dest_region_cd_indexCat", "block_id_indexCat", "trans_curr_cd_indexCat","mchnt_tp_indexCat") 

     val assembler1 = new VectorAssembler()
      .setInputCols(IndexeCatArr)
      .setOutputCol("features_Cat")
     
     var vec_data = assembler1.transform(transdata)
     vec_data.show(10)
     
      
     
     val normalizer2 = new Normalizer().setInputCol("features_Cat").setOutputCol("normFeatures")     //默认是L2
     val l2NormData = normalizer2.transform(vec_data)
     println("Normalize dataframe")
     l2NormData.show(10)
     
//     由于   VectorAssembler输出的应该是稀疏矩阵，而 spark 1.5的StandardScaler()还不支持稀疏矩阵。所以这里不能用。 spark2据说修复了
//     val scaler1 = new StandardScaler().setInputCol("features_Cat").setOutputCol("scaledFeatures").setWithMean(true).setWithStd(true)
//     val StandardData = scaler1.fit(vec_data).transform(vec_data)
//     println("StandardData dataframe")
//     StandardData.show(10)
//  
     val scaler2 = new MinMaxScaler().setInputCol("features_Cat").setOutputCol("scaledFeatures")
     val MinMaxData = scaler2.fit(vec_data).transform(vec_data)
     println("MinMaxData dataframe")
     MinMaxData.show(10)
   
     
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