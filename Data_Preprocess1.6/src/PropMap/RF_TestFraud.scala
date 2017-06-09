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
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.MultiClassSummarizer

object RF_TestFraud {
  

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
     
    val parsedRDD = sc.textFile("xrli/IntelDNN/Labeled_20160701.csv").map(_.split(",")).map(eachRow => {
     val a = eachRow.map(x => x.toDouble)
     Row(a(0),a(1),a(2),a(3),a(4),a(5),a(6),a(7),a(8),a(9),a(10),a(11),a(12),a(13),a(14),a(15),a(16),a(17),a(18),a(19),a(20),a(21),a(22),a(23),a(24),a(25),a(26),a(27),a(28),a(29),a(30),a(31),a(32),a(33),a(34),a(35),a(36),a(37),a(38),a(39),a(40),a(41),a(42),a(43),a(44),a(45),a(46),a(47),a(48),a(49),a(50),a(51),a(52),a(53),a(54),a(55),a(56),a(57),a(58),a(59),a(60),a(61),a(62),a(63),a(64),a(65),a(66),a(67),a(68),a(69),a(70),a(71),a(72),a(73),a(74),a(75),a(76),a(77),a(78),a(79),a(80),a(81),a(82),a(83),a(84),a(85),a(86),a(87),a(88))
    }
   )
    val sqlCtx = new SQLContext(sc)
    
    val schemacc =  StructType(StructField("isFraud",DoubleType,true)::StructField("tfr_dt_tm",DoubleType,true)::StructField("trans_at",DoubleType,true)::StructField("total_disc_at",DoubleType,true)::StructField("settle_tp_CatVec",DoubleType,true)::StructField("settle_cycle_CatVec",DoubleType,true)::StructField("block_id_CatVec",DoubleType,true)::StructField("trans_fwd_st_CatVec",DoubleType,true)::StructField("trans_rcv_st_CatVec",DoubleType,true)::StructField("sms_dms_conv_in_CatVec",DoubleType,true)::StructField("cross_dist_in_CatVec",DoubleType,true)::StructField("tfr_in_in_CatVec",DoubleType,true)::StructField("trans_md_CatVec",DoubleType,true)::StructField("source_region_cd_CatVec",DoubleType,true)::StructField("dest_region_cd_CatVec",DoubleType,true)::StructField("cups_card_in_CatVec",DoubleType,true)::StructField("cups_sig_card_in_CatVec",DoubleType,true)::StructField("card_class_CatVec",DoubleType,true)::StructField("card_attr_CatVec",DoubleType,true)::StructField("acq_ins_tp_CatVec",DoubleType,true)::StructField("fwd_ins_tp_CatVec",DoubleType,true)::StructField("rcv_ins_tp_CatVec",DoubleType,true)::StructField("iss_ins_tp_CatVec",DoubleType,true)::StructField("acpt_ins_tp_CatVec",DoubleType,true)::StructField("resp_cd1_CatVec",DoubleType,true)::StructField("resp_cd2_CatVec",DoubleType,true)::StructField("resp_cd3_CatVec",DoubleType,true)::StructField("resp_cd4_CatVec",DoubleType,true)::StructField("cu_trans_st_CatVec",DoubleType,true)::StructField("sti_takeout_in_CatVec",DoubleType,true)::StructField("trans_id_CatVec",DoubleType,true)::StructField("trans_tp_CatVec",DoubleType,true)::StructField("trans_chnl_CatVec",DoubleType,true)::StructField("card_media_CatVec",DoubleType,true)::StructField("card_brand_CatVec",DoubleType,true)::StructField("trans_id_conv_CatVec",DoubleType,true)::StructField("trans_curr_cd_CatVec",DoubleType,true)::StructField("conn_md_CatVec",DoubleType,true)::StructField("msg_tp_CatVec",DoubleType,true)::StructField("msg_tp_conv_CatVec",DoubleType,true)::StructField("trans_proc_cd_CatVec",DoubleType,true)::StructField("trans_proc_cd_conv_CatVec",DoubleType,true)::StructField("mchnt_tp_CatVec",DoubleType,true)::StructField("pos_entry_md_cd_CatVec",DoubleType,true)::StructField("card_seq_CatVec",DoubleType,true)::StructField("pos_cond_cd_CatVec",DoubleType,true)::StructField("pos_cond_cd_conv_CatVec",DoubleType,true)::StructField("term_tp_CatVec",DoubleType,true)::StructField("rsn_cd_CatVec",DoubleType,true)::StructField("addn_pos_inf_CatVec",DoubleType,true)::StructField("orig_msg_tp_CatVec",DoubleType,true)::StructField("orig_msg_tp_conv_CatVec",DoubleType,true)::StructField("related_trans_id_CatVec",DoubleType,true)::StructField("related_trans_chnl_CatVec",DoubleType,true)::StructField("orig_trans_id_CatVec",DoubleType,true)::StructField("orig_trans_chnl_CatVec",DoubleType,true)::StructField("orig_card_media_CatVec",DoubleType,true)::StructField("spec_settle_in_CatVec",DoubleType,true)::StructField("iss_ds_settle_in_CatVec",DoubleType,true)::StructField("acq_ds_settle_in_CatVec",DoubleType,true)::StructField("upd_in_CatVec",DoubleType,true)::StructField("exp_rsn_cd_CatVec",DoubleType,true)::StructField("pri_cycle_no_CatVec",DoubleType,true)::StructField("corr_pri_cycle_no_CatVec",DoubleType,true)::StructField("disc_in_CatVec",DoubleType,true)::StructField("orig_disc_curr_cd_CatVec",DoubleType,true)::StructField("fwd_settle_conv_rt_CatVec",DoubleType,true)::StructField("rcv_settle_conv_rt_CatVec",DoubleType,true)::StructField("fwd_settle_curr_cd_CatVec",DoubleType,true)::StructField("rcv_settle_curr_cd_CatVec",DoubleType,true)::StructField("sp_mchnt_cd_CatVec",DoubleType,true)::StructField("acq_ins_id_cd_BK_CatVec",DoubleType,true)::StructField("acq_ins_id_cd_RG_CatVec",DoubleType,true)::StructField("fwd_ins_id_cd_BK_CatVec",DoubleType,true)::StructField("fwd_ins_id_cd_RG_CatVec",DoubleType,true)::StructField("rcv_ins_id_cd_BK_CatVec",DoubleType,true)::StructField("rcv_ins_id_cd_RG_CatVec",DoubleType,true)::StructField("iss_ins_id_cd_BK_CatVec",DoubleType,true)::StructField("iss_ins_id_cd_RG_CatVec",DoubleType,true)::StructField("related_ins_id_cd_BK_CatVec",DoubleType,true)::StructField("related_ins_id_cd_RG_CatVec",DoubleType,true)::StructField("acpt_ins_id_cd_BK_CatVec",DoubleType,true)::StructField("acpt_ins_id_cd_RG_CatVec",DoubleType,true)::StructField("settle_fwd_ins_id_cd_BK_CatVec",DoubleType,true)::StructField("settle_fwd_ins_id_cd_RG_CatVec",DoubleType,true)::StructField("settle_rcv_ins_id_cd_BK_CatVec",DoubleType,true)::StructField("settle_rcv_ins_id_cd_RG_CatVec",DoubleType,true)::StructField("acct_ins_id_cd_BK_CatVec",DoubleType,true)::StructField("acct_ins_id_cd_RG_CatVec",DoubleType,true):: Nil)
    
    var vec_data = sqlCtx.createDataFrame(parsedRDD, schemacc).persist(StorageLevel.MEMORY_AND_DISK_SER)
 
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////     
    val CatVecArr =  Array("settle_tp_CatVec","settle_cycle_CatVec","block_id_CatVec","trans_fwd_st_CatVec","trans_rcv_st_CatVec","sms_dms_conv_in_CatVec","cross_dist_in_CatVec","tfr_in_in_CatVec","trans_md_CatVec","source_region_cd_CatVec","dest_region_cd_CatVec","cups_card_in_CatVec","cups_sig_card_in_CatVec","card_class_CatVec","card_attr_CatVec","acq_ins_tp_CatVec","fwd_ins_tp_CatVec","rcv_ins_tp_CatVec","iss_ins_tp_CatVec","acpt_ins_tp_CatVec","resp_cd1_CatVec","resp_cd2_CatVec","resp_cd3_CatVec","resp_cd4_CatVec","cu_trans_st_CatVec","sti_takeout_in_CatVec","trans_id_CatVec","trans_tp_CatVec","trans_chnl_CatVec","card_media_CatVec","card_brand_CatVec","trans_id_conv_CatVec","trans_curr_cd_CatVec","conn_md_CatVec","msg_tp_CatVec","msg_tp_conv_CatVec","trans_proc_cd_CatVec","trans_proc_cd_conv_CatVec","mchnt_tp_CatVec","pos_entry_md_cd_CatVec","card_seq_CatVec","pos_cond_cd_CatVec","pos_cond_cd_conv_CatVec","term_tp_CatVec","rsn_cd_CatVec","addn_pos_inf_CatVec","orig_msg_tp_CatVec","orig_msg_tp_conv_CatVec","related_trans_id_CatVec","related_trans_chnl_CatVec","orig_trans_id_CatVec","orig_trans_chnl_CatVec","orig_card_media_CatVec","spec_settle_in_CatVec","iss_ds_settle_in_CatVec","acq_ds_settle_in_CatVec","upd_in_CatVec","exp_rsn_cd_CatVec","pri_cycle_no_CatVec","corr_pri_cycle_no_CatVec","disc_in_CatVec","orig_disc_curr_cd_CatVec","fwd_settle_conv_rt_CatVec","rcv_settle_conv_rt_CatVec","fwd_settle_curr_cd_CatVec","rcv_settle_curr_cd_CatVec","sp_mchnt_cd_CatVec","acq_ins_id_cd_BK_CatVec","acq_ins_id_cd_RG_CatVec","fwd_ins_id_cd_BK_CatVec","fwd_ins_id_cd_RG_CatVec","rcv_ins_id_cd_BK_CatVec","rcv_ins_id_cd_RG_CatVec","iss_ins_id_cd_BK_CatVec","iss_ins_id_cd_RG_CatVec","related_ins_id_cd_BK_CatVec","related_ins_id_cd_RG_CatVec","acpt_ins_id_cd_BK_CatVec","acpt_ins_id_cd_RG_CatVec","settle_fwd_ins_id_cd_BK_CatVec","settle_fwd_ins_id_cd_RG_CatVec","settle_rcv_ins_id_cd_BK_CatVec","settle_rcv_ins_id_cd_RG_CatVec","acct_ins_id_cd_BK_CatVec","acct_ins_id_cd_RG_CatVec")
     
    val assembler1 = new VectorAssembler()
      .setInputCols(CatVecArr)
      .setOutputCol("featureVector")
     
      
    //laber_indexer一定要，否则报错
    //RandomForestClassifier was given input with invalid label column isFraud, without the number of classes specified. See StringIndexer.
    //https://stackoverflow.com/questions/36517302/randomforestclassifier-was-given-input-with-invalid-label-column-error-in-apache 
      
    val laber_indexer = new StringIndexer()
     .setInputCol("isFraud")
     .setOutputCol("label_idx")
     .fit(vec_data)  
       
      val rfClassifier = new RandomForestClassifier()
        .setLabelCol("label_idx")
        .setFeaturesCol("featureVector")
        .setNumTrees(5)
      
        
      val Array(trainingData, testData) = vec_data.randomSplit(Array(0.8, 0.2))
  
      val pipeline = new Pipeline().setStages(Array(assembler1,laber_indexer, rfClassifier))
      
      val model = pipeline.fit(trainingData)
  
      val evaluator_precision = new MulticlassClassificationEvaluator()
        .setLabelCol("label_idx")
        .setPredictionCol("prediction")
        .setMetricName("precision")     
        
      val evaluator_recall = new MulticlassClassificationEvaluator()
        .setLabelCol("label_idx")
        .setPredictionCol("prediction")
        .setMetricName("recall")
        
        //spark1  supports "f1" (default), "precision", "recall", "weightedPrecision", "weightedRecall"
        // spark2 dataframe  支持4种  supports "f1" (default), "weightedPrecision", "weightedRecall", "accuracy")    好像RDD多一点，http://blog.csdn.net/qq_34531825/article/details/52387513?locationNum=4
   
              
 //////////////////测试0。2的验证集的准确率//////////////////////////////////    
      val predictedData = model.transform(testData)
      val predictionAccuracy = evaluator_precision.evaluate(predictedData)
      println("Precision:" +  predictionAccuracy)
      
      val binaryClassificationEvaluator = new BinaryClassificationEvaluator()
      def printlnMetric(metricName: String): Unit = {
          println(metricName + " = " + binaryClassificationEvaluator.setLabelCol("label_idx").setMetricName(metricName).evaluate(predictedData))
      }

      printlnMetric("areaUnderROC")
      //printlnMetric("areaUnderPR")    //spark2.0

 //////////////////测试欺诈样本的准确率//////////////////////////////////    
      val frauddata_valid = vec_data.filter(vec_data("isFraud").===(0))
      val frauddata_valid_predict = model.transform(frauddata_valid)
      val prediction2 = evaluator_precision.evaluate(frauddata_valid_predict)
      println("frauddata_valid Precision:" +  prediction2)
       
      val prediction3 = evaluator_recall.evaluate(frauddata_valid_predict)
      println("frauddata_valid recall:" +  prediction3)
      
       
//////////////////////////////不调用，自己算混淆矩阵//////////////////////////////
      //分类正确且分类为1的样本数量 TP  
     val TP_Cnt = predictedData.filter(predictedData("label_idx") === predictedData("prediction")).filter(predictedData("label_idx")===1).count.toDouble

      //分类正确且分类为0的样本数量 TN  
     val TN_Cnt = predictedData.filter(predictedData("label_idx") === predictedData("prediction")).filter(predictedData("label_idx")===0).count.toDouble

      //分类错误且分类为1的样本数量 FP 
     val FP_Cnt = predictedData.filter(predictedData("label_idx") !== predictedData("prediction")).filter(predictedData("prediction")===1).count.toDouble

      //分类错误且分类为0的样本数量 FN 
     val FN_Cnt = predictedData.filter(predictedData("label_idx") !== predictedData("prediction")).filter(predictedData("prediction")===0).count.toDouble
     
     val Precision_N = TN_Cnt/(TN_Cnt + FN_Cnt)
     val Recall_N = TN_Cnt/(TN_Cnt + FP_Cnt)
      
     println("TP_Cnt is: " + TP_Cnt)
     println("TN_Cnt is: " + TN_Cnt)
     println("FP_Cnt is: " + FP_Cnt)
     println("FN_Cnt is: " + FN_Cnt)
     println("Precision_N is: " + Precision_N)
     println("Recall_N is: " + Recall_N)
     
     
     vec_data.unpersist(false)
 
  }
  
  
  
  def getPipeline(DisperseArr: Array[String]): ArrayBuffer[PipelineStage] = {
    val pipelineStages = new ArrayBuffer[PipelineStage]
    var i=0
    for (col <- DisperseArr) {
      i=i+1
      println(i)
      pipelineStages += new StringIndexer()
        .setInputCol(col + "_filled")
        .setOutputCol(col + "_CatVec")
        .setHandleInvalid("skip")
    }
    
    pipelineStages
  }
  
  
}