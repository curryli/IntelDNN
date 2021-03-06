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


object FraudRF {
  

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
 
    val startTime = System.currentTimeMillis(); 
    
    //注意select * 会出现内存溢出报错，估计是1行太多了，java堆栈不够，起始可以设置  http://blog.csdn.net/oaimm/article/details/25298691  但这里就少选几列就可以了
    var AllData = hc.sql(s"select settle_tp,settle_cycle,block_id,trans_fwd_st,trans_rcv_st,sms_dms_conv_in,cross_dist_in,tfr_in_in,trans_md,source_region_cd,dest_region_cd,cups_card_in,cups_sig_card_in,card_class,card_attr,acq_ins_tp,fwd_ins_tp,rcv_ins_tp,iss_ins_tp,acpt_ins_tp,resp_cd1,resp_cd2,resp_cd3,resp_cd4,cu_trans_st,sti_takeout_in,trans_id,trans_tp,trans_chnl,card_media,card_brand,trans_id_conv,trans_curr_cd,conn_md,msg_tp,msg_tp_conv,card_bin,related_card_bin,trans_proc_cd,trans_proc_cd_conv,mchnt_tp,pos_entry_md_cd,card_seq,pos_cond_cd,pos_cond_cd_conv,term_tp,rsn_cd,addn_pos_inf,orig_msg_tp,orig_msg_tp_conv,related_trans_id,related_trans_chnl,orig_trans_id,orig_trans_chnl,orig_card_media,spec_settle_in,iss_ds_settle_in,acq_ds_settle_in,upd_in,exp_rsn_cd,pri_cycle_no,corr_pri_cycle_no,disc_in,orig_disc_curr_cd,fwd_settle_conv_rt,rcv_settle_conv_rt,fwd_settle_curr_cd,rcv_settle_curr_cd,sp_mchnt_cd, " +    //mchnt_cd,
      s"substring(acq_ins_id_cd,1,4) as acq_ins_id_cd_BK, substring(acq_ins_id_cd,5,4) as acq_ins_id_cd_RG, " + 
      s"substring(fwd_ins_id_cd,1,4) as fwd_ins_id_cd_BK, substring(fwd_ins_id_cd,5,4) as fwd_ins_id_cd_RG, " + 
      s"substring(rcv_ins_id_cd,1,4) as rcv_ins_id_cd_BK, substring(rcv_ins_id_cd,5,4) as rcv_ins_id_cd_RG, " + 
      s"substring(iss_ins_id_cd,1,4) as iss_ins_id_cd_BK, substring(iss_ins_id_cd,5,4) as iss_ins_id_cd_RG, " + 
      s"substring(related_ins_id_cd,1,4) as related_ins_id_cd_BK, substring(related_ins_id_cd,5,4) as related_ins_id_cd_RG, " + 
      s"substring(acpt_ins_id_cd,1,4) acpt_ins_id_cd_BK, substring(acpt_ins_id_cd,5,4) as acpt_ins_id_cd_RG, " + 
      s"substring(settle_fwd_ins_id_cd,1,4) as settle_fwd_ins_id_cd_BK, substring(settle_fwd_ins_id_cd,5,4) as settle_fwd_ins_id_cd_RG, " + 
      s"substring(settle_rcv_ins_id_cd,1,4) as settle_rcv_ins_id_cd_BK, substring(settle_rcv_ins_id_cd,5,4) as settle_rcv_ins_id_cd_RG, " + 
      s"substring(acct_ins_id_cd,1,4) as acct_ins_id_cd_BK, substring(acct_ins_id_cd,5,4) as acct_ins_id_cd_RG " +
	    s"from tbl_common_his_trans where pdate>=20160701 and pdate<=20160701 ").cache           //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    println("AllData count is " + AllData.count())
    
    var FraudData = hc.sql(s"select t1.settle_tp,t1.settle_cycle,t1.block_id,t1.trans_fwd_st,t1.trans_rcv_st,t1.sms_dms_conv_in,t1.cross_dist_in,t1.tfr_in_in,t1.trans_md,t1.source_region_cd,t1.dest_region_cd,t1.cups_card_in,t1.cups_sig_card_in,t1.card_class,t1.card_attr,t1.acq_ins_tp,t1.fwd_ins_tp,t1.rcv_ins_tp,t1.iss_ins_tp,t1.acpt_ins_tp,t1.resp_cd1,t1.resp_cd2,t1.resp_cd3,t1.resp_cd4,t1.cu_trans_st,t1.sti_takeout_in,t1.trans_id,t1.trans_tp,t1.trans_chnl,t1.card_media,t1.card_brand,t1.trans_id_conv,t1.trans_curr_cd,t1.conn_md,t1.msg_tp,t1.msg_tp_conv,t1.card_bin,t1.related_card_bin,t1.trans_proc_cd,t1.trans_proc_cd_conv,t1.mchnt_tp,t1.pos_entry_md_cd,t1.card_seq,t1.pos_cond_cd,t1.pos_cond_cd_conv,t1.term_tp,t1.rsn_cd,t1.addn_pos_inf,t1.orig_msg_tp,t1.orig_msg_tp_conv,t1.related_trans_id,t1.related_trans_chnl,t1.orig_trans_id,t1.orig_trans_chnl,t1.orig_card_media,t1.spec_settle_in,t1.iss_ds_settle_in,t1.acq_ds_settle_in,t1.upd_in,t1.exp_rsn_cd,t1.pri_cycle_no,t1.corr_pri_cycle_no,t1.disc_in,t1.orig_disc_curr_cd,t1.fwd_settle_conv_rt,t1.rcv_settle_conv_rt,t1.fwd_settle_curr_cd,t1.rcv_settle_curr_cd,t1.sp_mchnt_cd, " +    //mchnt_cd,t1.
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
      s"where t1.pdate>=20160701 and t1.pdate<=20160701 ")
    println("FraudData count is " + FraudData.count())
    
    var NormalData = AllData.except(FraudData)
    println("NormalData count is " + NormalData.count())
    NormalData = NormalData.sample(false, 0.00005, 0) 
    
    val udf_Map0 = udf[Int, String]{xstr => 0}
    val udf_Map1 = udf[Int, String]{xstr => 1}
    
    FraudData = FraudData.withColumn("isFraud", udf_Map1(FraudData("trans_md")))
    FraudData.show(5)
    NormalData = NormalData.withColumn("isFraud", udf_Map0(NormalData("trans_md")))
    NormalData.show(5)
    

    
    var LabeledData = NormalData.unionAll(FraudData)
    
    
    val DisperseArr = Array("settle_tp","settle_cycle","block_id","trans_fwd_st","trans_rcv_st","sms_dms_conv_in","cross_dist_in","tfr_in_in","trans_md","source_region_cd","dest_region_cd","cups_card_in","cups_sig_card_in","card_class","card_attr","acq_ins_tp","fwd_ins_tp","rcv_ins_tp","iss_ins_tp","acpt_ins_tp","resp_cd1","resp_cd2","resp_cd3","resp_cd4","cu_trans_st","sti_takeout_in","trans_id","trans_tp","trans_chnl","card_media","card_brand","trans_id_conv","trans_curr_cd","conn_md","msg_tp","msg_tp_conv","trans_proc_cd","trans_proc_cd_conv","mchnt_tp","pos_entry_md_cd","card_seq","pos_cond_cd","pos_cond_cd_conv","term_tp","rsn_cd","addn_pos_inf","orig_msg_tp","orig_msg_tp_conv","related_trans_id","related_trans_chnl","orig_trans_id","orig_trans_chnl","orig_card_media","spec_settle_in","iss_ds_settle_in","acq_ds_settle_in","upd_in","exp_rsn_cd","pri_cycle_no","corr_pri_cycle_no","disc_in","orig_disc_curr_cd","fwd_settle_conv_rt","rcv_settle_conv_rt","fwd_settle_curr_cd","rcv_settle_curr_cd","sp_mchnt_cd","acq_ins_id_cd_BK","acq_ins_id_cd_RG","fwd_ins_id_cd_BK","fwd_ins_id_cd_RG","rcv_ins_id_cd_BK","rcv_ins_id_cd_RG","iss_ins_id_cd_BK","iss_ins_id_cd_RG","related_ins_id_cd_BK","related_ins_id_cd_RG","acpt_ins_id_cd_BK","acpt_ins_id_cd_RG","settle_fwd_ins_id_cd_BK","settle_fwd_ins_id_cd_RG","settle_rcv_ins_id_cd_BK","settle_rcv_ins_id_cd_RG","acct_ins_id_cd_BK","acct_ins_id_cd_RG")    //"card_bin","related_card_bin",
          
	   //val DisperseArr = Array("block_id", "source_region_cd", "dest_region_cd", "trans_curr_cd","mchnt_tp") 
	    
     val udf_replaceEmpty = udf[String, String]{xstr => 
        if(xstr.isEmpty())
          "NANs"
        else
          xstr
      }
 
     for(oldcol <- DisperseArr){
        val newcol = oldcol + "_filled" 
        LabeledData = LabeledData.withColumn(newcol, udf_replaceEmpty(LabeledData(oldcol)))
        //transdata = transdata.drop(oldcol)
     }
     println("NaNs filled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
     
     var i = 0
     for(oldcol <- DisperseArr){   
        println(i)
        i = i + 1 
        val newcol = oldcol + "_filled" 
        var indexCat = oldcol + "_CatVec"
        var indexer = new StringIndexer().setInputCol(newcol).setOutputCol(indexCat).setHandleInvalid("skip")
        LabeledData = indexer.fit(LabeledData).transform(LabeledData)
      }
    
     val CatVecArr = Array("settle_tp_CatVec","settle_cycle_CatVec","block_id_CatVec","trans_fwd_st_CatVec","trans_rcv_st_CatVec","sms_dms_conv_in_CatVec","cross_dist_in_CatVec","tfr_in_in_CatVec","trans_md_CatVec","source_region_cd_CatVec","dest_region_cd_CatVec","cups_card_in_CatVec","cups_sig_card_in_CatVec","card_class_CatVec","card_attr_CatVec","acq_ins_tp_CatVec","fwd_ins_tp_CatVec","rcv_ins_tp_CatVec","iss_ins_tp_CatVec","acpt_ins_tp_CatVec","resp_cd1_CatVec","resp_cd2_CatVec","resp_cd3_CatVec","resp_cd4_CatVec","cu_trans_st_CatVec","sti_takeout_in_CatVec","trans_id_CatVec","trans_tp_CatVec","trans_chnl_CatVec","card_media_CatVec","card_brand_CatVec","trans_id_conv_CatVec","trans_curr_cd_CatVec","conn_md_CatVec","msg_tp_CatVec","msg_tp_conv_CatVec","trans_proc_cd_CatVec","trans_proc_cd_conv_CatVec","mchnt_tp_CatVec","pos_entry_md_cd_CatVec","card_seq_CatVec","pos_cond_cd_CatVec","pos_cond_cd_conv_CatVec","term_tp_CatVec","rsn_cd_CatVec","addn_pos_inf_CatVec","orig_msg_tp_CatVec","orig_msg_tp_conv_CatVec","related_trans_id_CatVec","related_trans_chnl_CatVec","orig_trans_id_CatVec","orig_trans_chnl_CatVec","orig_card_media_CatVec","spec_settle_in_CatVec","iss_ds_settle_in_CatVec","acq_ds_settle_in_CatVec","upd_in_CatVec","exp_rsn_cd_CatVec","pri_cycle_no_CatVec","corr_pri_cycle_no_CatVec","disc_in_CatVec","orig_disc_curr_cd_CatVec","fwd_settle_conv_rt_CatVec","rcv_settle_conv_rt_CatVec","fwd_settle_curr_cd_CatVec","rcv_settle_curr_cd_CatVec","sp_mchnt_cd_CatVec","acq_ins_id_cd_BK_CatVec","acq_ins_id_cd_RG_CatVec","fwd_ins_id_cd_BK_CatVec","fwd_ins_id_cd_RG_CatVec","rcv_ins_id_cd_BK_CatVec","rcv_ins_id_cd_RG_CatVec","iss_ins_id_cd_BK_CatVec","iss_ins_id_cd_RG_CatVec","related_ins_id_cd_BK_CatVec","related_ins_id_cd_RG_CatVec","acpt_ins_id_cd_BK_CatVec","acpt_ins_id_cd_RG_CatVec","settle_fwd_ins_id_cd_BK_CatVec","settle_fwd_ins_id_cd_RG_CatVec","settle_rcv_ins_id_cd_BK_CatVec","settle_rcv_ins_id_cd_RG_CatVec","acct_ins_id_cd_BK_CatVec","acct_ins_id_cd_RG_CatVec")   //"card_bin_CatVec","related_card_bin_CatVec",
  
      val assembler1 = new VectorAssembler()
      .setInputCols(CatVecArr)
      .setOutputCol("featureVector")
     
     var vec_data = assembler1.transform(LabeledData)
     vec_data.select("featureVector").show(5)
     
     
      val rfClassifier = new RandomForestClassifier()
        .setLabelCol("isFraud")
        .setFeaturesCol("featureVector")
        .setNumTrees(5)
      
         
      val Array(trainingData, testData) = vec_data.randomSplit(Array(0.8, 0.2))
  
      val pipeline = new Pipeline().setStages(Array(assembler1,rfClassifier))
      
      val model = pipeline.fit(trainingData)
      
      val predictionResultDF = model.transform(testData)
      
      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("isFraud")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")     //选择一哪种方式进行评估      (目前dataframe  支持4种  supports "f1" (default), "weightedPrecision", "weightedRecall", "accuracy")    好像RDD多一点，http://blog.csdn.net/qq_34531825/article/details/52387513?locationNum=4
 
      val predictionAccuracy = evaluator.evaluate(predictionResultDF)
      println("Testing Error = " + (1.0 - predictionAccuracy))
 
  }
  
  
  
  
  
  
}