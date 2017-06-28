package Prepare
import org.apache.log4j.Level
import org.apache.log4j.Logger
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
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.QuantileDiscretizer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map 

object dummy_map {
  val QD_money_num = 10
  val QD_disc_num = 10
  val NAN_Arr = IntelUtil.constUtil.NAN_Arr 

  def main(args: Array[String]): Unit = {

    //屏蔽日志
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR); 
    
    //val sparkConf = new SparkConf().setAppName("spark2SQL")
    val warehouseLocation = "spark-warehouse"
    
    val ss = SparkSession
      .builder()
      .appName("Save_IndexerPipeLine")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.schema.verification", false)
      .getOrCreate()
  
    import ss.implicits._
    import ss.sql
 
    val startTime = System.currentTimeMillis(); 
 
    val rangedir = IntelUtil.varUtil.rangeDir 
     
    var input_dir = rangedir + "Labeled_All"
    var labeled_noNAN = IntelUtil.get_from_HDFS.get_labeled_noNAN(ss, input_dir).cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    labeled_noNAN.show(5)
     
        //for(colname<-Array("settle_tp_filled")){
   for(colname<-IntelUtil.constUtil.dummy_Arr){
      println(colname)
      var tmpMap = labeled_noNAN.select(colname).distinct().rdd.map(r => r.getString(0)).collect().zipWithIndex.toMap      
      println(colname + ": " + tmpMap.keys.size)

      val udf_map = udf[Int, String]{xstr => tmpMap(xstr)}
      var newcol = colname + "_dummy"
      labeled_noNAN = labeled_noNAN.withColumn(newcol, udf_map(labeled_noNAN(colname)))
      
   }
    
    labeled_noNAN.show(5)
    
    
    
    
    
    val CatVecArr = Array("day_week_filled","hour_filled", "trans_at_filled", "total_disc_at_filled", "settle_tp_filled_dummy","settle_cycle_filled_dummy","block_id_filled_dummy","trans_fwd_st_filled_dummy","trans_rcv_st_filled_dummy","sms_dms_conv_in_filled_dummy","cross_dist_in_filled_dummy","tfr_in_in_filled_dummy","trans_md_filled_dummy","source_region_cd_filled_dummy","dest_region_cd_filled_dummy","cups_card_in_filled_dummy","cups_sig_card_in_filled_dummy","card_class_filled_dummy","card_attr_filled_dummy","acq_ins_tp_filled_dummy","fwd_ins_tp_filled_dummy","rcv_ins_tp_filled_dummy","iss_ins_tp_filled_dummy","acpt_ins_tp_filled_dummy","resp_cd1_filled_dummy","resp_cd2_filled_dummy","resp_cd3_filled_dummy","resp_cd4_filled_dummy","cu_trans_st_filled_dummy","sti_takeout_in_filled_dummy","trans_id_filled_dummy","trans_tp_filled_dummy","trans_chnl_filled_dummy","card_media_filled_dummy","trans_id_conv_filled_dummy","trans_curr_cd_filled_dummy","conn_md_filled_dummy","msg_tp_filled_dummy","msg_tp_conv_filled_dummy","trans_proc_cd_filled_dummy","trans_proc_cd_conv_filled_dummy","mchnt_tp_filled_dummy","pos_entry_md_cd_filled_dummy","pos_cond_cd_filled_dummy","pos_cond_cd_conv_filled_dummy","term_tp_filled_dummy","rsn_cd_filled_dummy","addn_pos_inf_filled_dummy","iss_ds_settle_in_filled_dummy","acq_ds_settle_in_filled_dummy","upd_in_filled_dummy","pri_cycle_no_filled_dummy","disc_in_filled_dummy","fwd_settle_conv_rt_filled_dummy","rcv_settle_conv_rt_filled_dummy","fwd_settle_curr_cd_filled_dummy","rcv_settle_curr_cd_filled_dummy","acq_ins_id_cd_BK_filled_dummy","acq_ins_id_cd_RG_filled_dummy","fwd_ins_id_cd_BK_filled_dummy","fwd_ins_id_cd_RG_filled_dummy","rcv_ins_id_cd_BK_filled_dummy","rcv_ins_id_cd_RG_filled_dummy","iss_ins_id_cd_BK_filled_dummy","iss_ins_id_cd_RG_filled_dummy","acpt_ins_id_cd_BK_filled_dummy","acpt_ins_id_cd_RG_filled_dummy","settle_fwd_ins_id_cd_BK_filled_dummy","settle_fwd_ins_id_cd_RG_filled_dummy","settle_rcv_ins_id_cd_BK_filled_dummy","settle_rcv_ins_id_cd_RG_filled_dummy") 
 
    val assembler = new VectorAssembler()
      .setInputCols(CatVecArr)
      .setOutputCol("featureVector")
    
    val label_indexer = new StringIndexer()
     .setInputCol("label_filled")
     .setOutputCol("label_idx")
     .fit(labeled_noNAN)  
       
      
      val rfClassifier = new RandomForestClassifier()
        .setLabelCol("label_idx")
        .setFeaturesCol("featureVector")
        .setNumTrees(5)
        .setMaxBins(10000)
        //.setThresholds(Array(10,1))
        //为每个分类设置一个阈值，参数的长度必须和类的个数相等。最终的分类结果会是p/t最大的那个分类，其中p是通过Bayes计算出来的结果，t是阈值。 
        //这对于训练样本严重不均衡的情况尤其重要，比如分类0有200万数据，而分类1有2万数据，此时应用new NaiveBayes().setThresholds(Array(100.0,1.0))    这里t1=100  t2=1
      
      val pipeline = new Pipeline().setStages(Array(assembler,label_indexer, rfClassifier))
      
       val Array(trainingData, testData) = labeled_noNAN.randomSplit(Array(0.8, 0.2))
      
      val model = pipeline.fit(trainingData)
      
      val predictionResultDF = model.transform(testData)
        
      val eval_result = IntelUtil.funUtil.get_CF_Matrix(predictionResultDF)
       
     println("Current Precision_P is: " + eval_result.Precision_P)
     println("Current Recall_P is: " + eval_result.Recall_P)
     
     
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  
  
    
}