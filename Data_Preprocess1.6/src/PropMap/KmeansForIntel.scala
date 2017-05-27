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
import org.apache.spark.ml.clustering.KMeans
//import org.apache.spark.mllib.clustering.KMeans
 


object  KmeansForIntel{
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);
    
   val conf = new SparkConf().setAppName("compare_2time")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
     
   val startTime = System.currentTimeMillis(); 
   
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
	    s"from tbl_common_his_trans where pdate=20160701 and substring(acpt_ins_id_cd,5,4)=3940 ").cache
  
	    println("SQL done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
      println("transdata.count is: " + transdata.count())
      
     val DisperseArr = Array("settle_tp","settle_cycle","block_id","trans_fwd_st","trans_rcv_st","sms_dms_conv_in","cross_dist_in","tfr_in_in","trans_md","source_region_cd","dest_region_cd","cups_card_in","cups_sig_card_in","card_class","card_attr","acq_ins_tp","fwd_ins_tp","rcv_ins_tp","iss_ins_tp","acpt_ins_tp","resp_cd1","resp_cd2","resp_cd3","resp_cd4","cu_trans_st","sti_takeout_in","trans_id","trans_tp","trans_chnl","card_media","card_brand","trans_id_conv","trans_curr_cd","conn_md","msg_tp","msg_tp_conv","trans_proc_cd","trans_proc_cd_conv","mchnt_tp","pos_entry_md_cd","card_seq","pos_cond_cd","pos_cond_cd_conv","term_tp","rsn_cd","addn_pos_inf","orig_msg_tp","orig_msg_tp_conv","related_trans_id","related_trans_chnl","orig_trans_id","orig_trans_chnl","orig_card_media","spec_settle_in","iss_ds_settle_in","acq_ds_settle_in","upd_in","exp_rsn_cd","pri_cycle_no","corr_pri_cycle_no","disc_in","orig_disc_curr_cd","fwd_settle_conv_rt","rcv_settle_conv_rt","fwd_settle_curr_cd","rcv_settle_curr_cd","sp_mchnt_cd","trans_media","acq_ins_id_cd_BK","acq_ins_id_cd_RG","fwd_ins_id_cd_BK","fwd_ins_id_cd_RG","rcv_ins_id_cd_BK","rcv_ins_id_cd_RG","iss_ins_id_cd_BK","iss_ins_id_cd_RG","related_ins_id_cd_BK","related_ins_id_cd_RG","acpt_ins_id_cd_BK","acpt_ins_id_cd_RG","settle_fwd_ins_id_cd_BK","settle_fwd_ins_id_cd_RG","settle_rcv_ins_id_cd_BK","settle_rcv_ins_id_cd_RG","acct_ins_id_cd_BK","acct_ins_id_cd_RG")    //"card_bin","related_card_bin",

     val udf_replaceEmpty = udf[String, String]{xstr => 
        if(xstr.isEmpty())
          "NANs"
        else
          xstr
      }
   
     
     for(oldcol <- DisperseArr){
        val newcol = oldcol + "_filled" 
        transdata = transdata.withColumn(newcol, udf_replaceEmpty(transdata(oldcol)))
     }
     println("NaNs filled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
     
     var i = 0
     for(oldcol <- DisperseArr){   
        println(i)
        i = i + 1 
        val newcol = oldcol + "_filled" 
        var indexCat = oldcol + "_CatVec"
        var indexer = new StringIndexer().setInputCol(newcol).setOutputCol(indexCat).setHandleInvalid("skip")
        transdata = indexer.fit(transdata).transform(transdata)
      }
      
    transdata.show(10)  
    
    println("StringIndexer done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
  
    val CatVecArr = Array("settle_tp_CatVec","settle_cycle_CatVec","block_id_CatVec","trans_fwd_st_CatVec","trans_rcv_st_CatVec","sms_dms_conv_in_CatVec","cross_dist_in_CatVec","tfr_in_in_CatVec","trans_md_CatVec","source_region_cd_CatVec","dest_region_cd_CatVec","cups_card_in_CatVec","cups_sig_card_in_CatVec","card_class_CatVec","card_attr_CatVec","acq_ins_tp_CatVec","fwd_ins_tp_CatVec","rcv_ins_tp_CatVec","iss_ins_tp_CatVec","acpt_ins_tp_CatVec","resp_cd1_CatVec","resp_cd2_CatVec","resp_cd3_CatVec","resp_cd4_CatVec","cu_trans_st_CatVec","sti_takeout_in_CatVec","trans_id_CatVec","trans_tp_CatVec","trans_chnl_CatVec","card_media_CatVec","card_brand_CatVec","trans_id_conv_CatVec","trans_curr_cd_CatVec","conn_md_CatVec","msg_tp_CatVec","msg_tp_conv_CatVec","trans_proc_cd_CatVec","trans_proc_cd_conv_CatVec","mchnt_tp_CatVec","pos_entry_md_cd_CatVec","card_seq_CatVec","pos_cond_cd_CatVec","pos_cond_cd_conv_CatVec","term_tp_CatVec","rsn_cd_CatVec","addn_pos_inf_CatVec","orig_msg_tp_CatVec","orig_msg_tp_conv_CatVec","related_trans_id_CatVec","related_trans_chnl_CatVec","orig_trans_id_CatVec","orig_trans_chnl_CatVec","orig_card_media_CatVec","spec_settle_in_CatVec","iss_ds_settle_in_CatVec","acq_ds_settle_in_CatVec","upd_in_CatVec","exp_rsn_cd_CatVec","pri_cycle_no_CatVec","corr_pri_cycle_no_CatVec","disc_in_CatVec","orig_disc_curr_cd_CatVec","fwd_settle_conv_rt_CatVec","rcv_settle_conv_rt_CatVec","fwd_settle_curr_cd_CatVec","rcv_settle_curr_cd_CatVec","sp_mchnt_cd_CatVec","trans_media_CatVec","acq_ins_id_cd_BK_CatVec","acq_ins_id_cd_RG_CatVec","fwd_ins_id_cd_BK_CatVec","fwd_ins_id_cd_RG_CatVec","rcv_ins_id_cd_BK_CatVec","rcv_ins_id_cd_RG_CatVec","iss_ins_id_cd_BK_CatVec","iss_ins_id_cd_RG_CatVec","related_ins_id_cd_BK_CatVec","related_ins_id_cd_RG_CatVec","acpt_ins_id_cd_BK_CatVec","acpt_ins_id_cd_RG_CatVec","settle_fwd_ins_id_cd_BK_CatVec","settle_fwd_ins_id_cd_RG_CatVec","settle_rcv_ins_id_cd_BK_CatVec","settle_rcv_ins_id_cd_RG_CatVec","acct_ins_id_cd_BK_CatVec","acct_ins_id_cd_RG_CatVec")   //"card_bin_CatVec","related_card_bin_CatVec",
 

//    transdata.select("settle_tp_CatVec","settle_cycle_CatVec","block_id_CatVec","trans_fwd_st_CatVec","trans_rcv_st_CatVec","sms_dms_conv_in_CatVec","cross_dist_in_CatVec","tfr_in_in_CatVec","trans_md_CatVec","source_region_cd_CatVec","dest_region_cd_CatVec","cups_card_in_CatVec","cups_sig_card_in_CatVec","card_class_CatVec","card_attr_CatVec","acq_ins_tp_CatVec","fwd_ins_tp_CatVec","rcv_ins_tp_CatVec","iss_ins_tp_CatVec","acpt_ins_tp_CatVec","resp_cd1_CatVec","resp_cd2_CatVec","resp_cd3_CatVec","resp_cd4_CatVec","cu_trans_st_CatVec","sti_takeout_in_CatVec","trans_id_CatVec","trans_tp_CatVec","trans_chnl_CatVec","card_media_CatVec","card_brand_CatVec","trans_id_conv_CatVec","trans_curr_cd_CatVec","conn_md_CatVec","msg_tp_CatVec","msg_tp_conv_CatVec","trans_proc_cd_CatVec","trans_proc_cd_conv_CatVec","mchnt_tp_CatVec","pos_entry_md_cd_CatVec","card_seq_CatVec","pos_cond_cd_CatVec","pos_cond_cd_conv_CatVec","term_tp_CatVec","rsn_cd_CatVec","addn_pos_inf_CatVec","orig_msg_tp_CatVec","orig_msg_tp_conv_CatVec","related_trans_id_CatVec","related_trans_chnl_CatVec","orig_trans_id_CatVec","orig_trans_chnl_CatVec","orig_card_media_CatVec","spec_settle_in_CatVec","iss_ds_settle_in_CatVec","acq_ds_settle_in_CatVec","upd_in_CatVec","exp_rsn_cd_CatVec","pri_cycle_no_CatVec","corr_pri_cycle_no_CatVec","disc_in_CatVec","orig_disc_curr_cd_CatVec","fwd_settle_conv_rt_CatVec","rcv_settle_conv_rt_CatVec","fwd_settle_curr_cd_CatVec","rcv_settle_curr_cd_CatVec","sp_mchnt_cd_CatVec","trans_media_CatVec","acq_ins_id_cd_BK_CatVec","acq_ins_id_cd_RG_CatVec","fwd_ins_id_cd_BK_CatVec","fwd_ins_id_cd_RG_CatVec","rcv_ins_id_cd_BK_CatVec","rcv_ins_id_cd_RG_CatVec","iss_ins_id_cd_BK_CatVec","iss_ins_id_cd_RG_CatVec","related_ins_id_cd_BK_CatVec","related_ins_id_cd_RG_CatVec","acpt_ins_id_cd_BK_CatVec","acpt_ins_id_cd_RG_CatVec","settle_fwd_ins_id_cd_BK_CatVec","settle_fwd_ins_id_cd_RG_CatVec","settle_rcv_ins_id_cd_BK_CatVec","settle_rcv_ins_id_cd_RG_CatVec","acct_ins_id_cd_BK_CatVec","acct_ins_id_cd_RG_CatVec")   
//                .saveAsTable("Intel_indexedTable")
 
    println("Save hive table done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
   
     val assembler1 = new VectorAssembler()
      .setInputCols(CatVecArr)
      .setOutputCol("features_Cat")
     
     var vec_data = assembler1.transform(transdata)
     vec_data.select("features_Cat").show(5)
     
     
     val scaler2 = new MinMaxScaler().setInputCol("features_Cat").setOutputCol("scaledFeatures")
     val MinMaxData = scaler2.fit(vec_data).transform(vec_data)
     println("MinMaxData dataframe")
     MinMaxData.show(10)
     
    println("MinMax done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
  
    val kmeans = new KMeans().setK(10).setSeed(1L).setFeaturesCol("features_Cat").setPredictionCol("prediction") 
    val model = kmeans.fit(vec_data)
    var current_cost = model.computeCost(vec_data)
    println("current_cost is : " +  current_cost)
    
        // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    
    val KmeansResult = model.transform(vec_data)
     println("KmeansResult: ")
    KmeansResult.show(5)
    
    var dfresult_1 = KmeansResult.filter(KmeansResult("prediction")===1)   
    dfresult_1.rdd.saveAsTextFile("xrli/IntelDNN/KmeansResult_Intel")
 
     
    //df.write.csv("xrli/TeleFraud/testKmeansResult")
    
    
    println("kmeans done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
  
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
      
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