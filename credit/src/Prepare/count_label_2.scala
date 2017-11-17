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
import scala.reflect.ClassTag


import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.feature.ChiSqSelectorModel



object count_label_2 {

  def any_to_double[T: ClassTag](b: T):Double={
    if(b==true)
      1.0
    else
      0
  }
  
   
  def main(args: Array[String]): Unit = {

    //屏蔽日志
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.ERROR);
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
 
    var labeledData = IntelUtil.get_from_HDFS.get_FE_DF(ss, "xrli/credit/cert_all_right").persist(StorageLevel.MEMORY_AND_DISK_SER)  
    
    labeledData = labeledData.na.fill(-1)
    
    //println("高危商户")
    println("is_HR_CA")
    val HighRisk_CAs = Array(4,33, 48, 69, 61, 76,71,43,70,46, 77,72, 94,67, 219, 43,46,103,188,160,180,213,225,209,219,281,296,270,337,316,317,428,344,326,471,466,555,459,480,589,592,523,569,472,595,601,618,632,937,572,712,700,661,579,941,1448,703,1497,1315,874,1185,1472,1576,714,1912,1017,2231,1578,1646,1536,1612,1041)
    val is_HR_CA = udf[Double, Double]{xstr => any_to_double(HighRisk_CAs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_CA", is_HR_CA(labeledData("card_accprt_nm_loc_filled_idx")))
    val not_HR_CA = udf[Double, Double]{xstr => any_to_double(!HighRisk_CAs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_CA", not_HR_CA(labeledData("card_accprt_nm_loc_filled_idx")))
    
      
    println("is_HR_AIRC")
    val HR_AIRCs = Array(37,83,1429,188,1387,525,418)
    val is_HR_AIRC = udf[Double, Double]{xstr => any_to_double(HR_AIRCs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_AIRC", is_HR_AIRC(labeledData("auth_id_resp_cd_filled_idx")))
    val not_HR_AIRC = udf[Double, Double]{xstr => any_to_double(!HR_AIRCs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_AIRC", not_HR_AIRC(labeledData("auth_id_resp_cd_filled_idx")))
    
    
    println("is_HR_MC")
    val HR_MCs = Array(5,50,45,58,247,219,192,285,262,342,358,395,344,453,390,563,433,462,623,465,621,436,928,749,688,809,827,671,1058,946,1031,882,957,1198,1120,1038,1114,1346,1161,1352,1168,1189,1201,1310,1306,922,1660,1365,1831,1485,945,934,1766,1587,1332)
    val is_HR_MC = udf[Double, Double]{xstr => any_to_double(HR_MCs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_MC", is_HR_MC(labeledData("mchnt_cd_filled_idx")))
    val not_HR_MC = udf[Double, Double]{xstr => any_to_double(!HR_MCs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_MC", not_HR_MC(labeledData("mchnt_cd_filled_idx")))
     
    println("is_HR_Term")
    val HR_Terms = Array(638,596,867,348,710,902,1050,1057,890)
    val is_HR_Term = udf[Double, Double]{xstr => any_to_double(HR_Terms.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_Term", is_HR_Term(labeledData("term_cd_filled_idx")))
    val not_HR_Term = udf[Double, Double]{xstr => any_to_double(!HR_Terms.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_Term", not_HR_Term(labeledData("term_cd_filled_idx")))
    
    
   println("is_HR_CA_2") 
   var tempDF = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/highrisk_tables/card_accprt_nm_loc_filled_idx.csv/part-00000-90e03786-2fcd-4f98-b677-d7ce934fcf1b.csv")
   var HighRisk_CAs_2 = tempDF.select("card_accprt_nm_loc_filled_idx_label").rdd.map(r=>r.getDouble(0)).collect()
   val is_HR_CA_2 = udf[Double, Double]{xstr => any_to_double(HighRisk_CAs_2.contains(xstr.toInt))}    
   labeledData = labeledData.withColumn("is_HR_CA_2", is_HR_CA_2(labeledData("card_accprt_nm_loc_filled_idx")))
   val not_HR_CA_2 = udf[Double, Double]{xstr => any_to_double(!HighRisk_CAs_2.contains(xstr.toInt))}    
   labeledData = labeledData.withColumn("not_HR_CA_2", not_HR_CA_2(labeledData("card_accprt_nm_loc_filled_idx")))
   
   println("is_HR_AIRC_2") 
   tempDF = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/highrisk_tables/auth_id_resp_cd_filled_idx.csv/part-00000-bf8e61af-66df-480f-b575-68c914a0dade.csv")
   var HR_AIRC_2 = tempDF.select("auth_id_resp_cd_filled_idx_label").rdd.map(r=>r.getDouble(0)).collect()
   val is_HR_AIRC_2 = udf[Double, Double]{xstr => any_to_double(HR_AIRC_2.contains(xstr.toInt))}    
   labeledData = labeledData.withColumn("is_HR_AIRC_2", is_HR_AIRC_2(labeledData("auth_id_resp_cd_filled_idx")))
   val not_HR_AIRC_2 = udf[Double, Double]{xstr => any_to_double(!HR_AIRC_2.contains(xstr.toInt))}    
   labeledData = labeledData.withColumn("not_HR_AIRC_2", not_HR_AIRC_2(labeledData("auth_id_resp_cd_filled_idx")))
    
  
   println("is_HR_MC_2") 
   tempDF = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/highrisk_tables/mchnt_cd_filled_idx.csv/part-00000-378dbad8-dce6-41fa-8bfd-848fab86604f.csv")
   var HR_MC_2 = tempDF.select("mchnt_cd_filled_idx_label").rdd.map(r=>r.getDouble(0)).collect()
   val is_HR_MC_2 = udf[Double, Double]{xstr => any_to_double(HR_MC_2.contains(xstr.toInt))}    
   labeledData = labeledData.withColumn("is_HR_MC_2", is_HR_MC_2(labeledData("mchnt_cd_filled_idx")))
   val not_HR_MC_2 = udf[Double, Double]{xstr => any_to_double(!HR_MC_2.contains(xstr.toInt))}    
   labeledData = labeledData.withColumn("not_HR_MC_2", not_HR_MC_2(labeledData("mchnt_cd_filled_idx")))
   
   println("is_HR_Term_2") 
   tempDF = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/highrisk_tables/term_cd_filled_idx.csv/part-00000-a6b34f64-97bd-40bd-9f58-e31ab062434b.csv")
   var HR_Term_2 = tempDF.select("term_cd_filled_idx_label").rdd.map(r=>r.getDouble(0)).collect()
   val is_HR_Term_2 = udf[Double, Double]{xstr => any_to_double(HR_Term_2.contains(xstr.toInt))}    
   labeledData = labeledData.withColumn("is_HR_Term_2", is_HR_Term_2(labeledData("term_cd_filled_idx")))
   val not_HR_Term_2 = udf[Double, Double]{xstr => any_to_double(!HR_Term_2.contains(xstr.toInt))}    
   labeledData = labeledData.withColumn("not_HR_Term_2", not_HR_Term_2(labeledData("term_cd_filled_idx")))
   
   
   
    
    println("is_HR_TS") 
    val is_HR_TS = udf[Double, Double]{xstr => any_to_double(xstr.toDouble>7)}    
    labeledData = labeledData.withColumn("is_HR_TS", is_HR_TS(labeledData("trans_st_filled_idx")))
    val not_HR_TS = udf[Double, Double]{xstr => any_to_double(xstr.toDouble<=7)}    
    labeledData = labeledData.withColumn("not_HR_TS", not_HR_TS(labeledData("trans_st_filled_idx")))
   
 
    println("is_HR_Tccs")
    val HR_Tccs = Array(0,2,5,1,8,3,4,6,12,10)
    val is_HR_Tccs = udf[Double, Double]{xstr => any_to_double(HR_Tccs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_Tccs", is_HR_Tccs(labeledData("trans_curr_cd_filled_idx")))
    val not_HR_Tccs = udf[Double, Double]{xstr => any_to_double(!HR_Tccs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_Tccs", not_HR_Tccs(labeledData("trans_curr_cd_filled_idx")))
    
    println("is_HR_Rc")
    val HR_Rcs = Array(0,1,2,3,5,7,8,4,9,13,6,11,10,12,19,14,21,17,18,15,16,20,31,24,23,22,25,43,30,38,29,41,32,26,28,27,44,82,77,121,126,150,193,123,42,78,64,100,242,86,201,111,183,276,223,36,39,54,231,118,84,58)
    val is_HR_Rc = udf[Double, Double]{xstr => any_to_double(HR_Rcs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_Rc", is_HR_Rc(labeledData("resp_cd_filled_idx")))
    val not_HR_Rc = udf[Double, Double]{xstr => any_to_double(!HR_Rcs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_Rc", not_HR_Rc(labeledData("resp_cd_filled_idx")))
    
   println("is_HR_RScc")
    val HR_RSccs = Array(0,2,3,10,4,26,7,68,101,16,11,149,9,147,82,157,506,382,446,395,276,248,619,33,245,539,1,17,398,336,231,558)
    val is_HR_RScc = udf[Double, Double]{xstr => any_to_double(HR_RSccs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_RScc", is_HR_RScc(labeledData("rcv_settle_curr_cd_filled_idx")))
    val not_HR_RScc = udf[Double, Double]{xstr => any_to_double(!HR_RSccs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_RScc", not_HR_RScc(labeledData("rcv_settle_curr_cd_filled_idx")))
    
    println("is_HR_RSCR")
    val HR_RSCRs = Array(0,1,2,141,215,250,393,371,191,507,351,6,72,680,453,210,648,55,7,674,712,25,201,151,640,83,869,645,912,164,68,354,387,370,579,756,242,465,775,490,852,65,3,271,53,148,290,958,439,925,522,1147,939,180,327,234,538,119,737,125,516,106,281,1132,41,157,48,977,71,291,1410,898,260,304,1251,321,382,666,472,356,342,404,1155,213,1280,407,66,1411,4,461,691,701,74,245,402,13,884,688,239,704,27,540,1519,1566,282,29,793,499,153,9,920,1266,891,144,1042,863,1092,950,1370,956,26,520,1053,118,59,578,1185,415,244,1320,599,789,176)
    val is_HR_RSCR = udf[Double, Double]{xstr => any_to_double(HR_RSCRs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_RSCR", is_HR_RSCR(labeledData("rcv_settle_conv_rt_filled_idx")))
    val not_HR_RSCR = udf[Double, Double]{xstr => any_to_double(!HR_RSCRs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_RSCR", not_HR_RSCR(labeledData("rcv_settle_conv_rt_filled_idx")))
    
    
     println("is_HR_pemc")
    val is_HR_pemc = udf[Double, Double]{xstr => any_to_double(xstr.toDouble>20)}    
    labeledData = labeledData.withColumn("is_HR_pemc", is_HR_pemc(labeledData("pos_entry_md_cd_filled_idx")))
    val not_HR_pemc = udf[Double, Double]{xstr => any_to_double(xstr.toDouble<=20)}    
    labeledData = labeledData.withColumn("not_HR_pemc", not_HR_pemc(labeledData("pos_entry_md_cd_filled_idx")))
    
     println("is_HR_Pcc")
    val HR_Pccs = Array(0,2,5,1,8,3,4,6,12,10)
    val is_HR_Pcc = udf[Double, Double]{xstr => any_to_double(HR_Pccs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_Pcc", is_HR_Pcc(labeledData("pos_cond_cd_filled_idx")))
    val not_HR_Pcc = udf[Double, Double]{xstr => any_to_double(!HR_Pccs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_Pcc", not_HR_Pcc(labeledData("pos_cond_cd_filled_idx")))
    
     println("is_HR_OTS")
    val is_HR_OTS = udf[Double, Double]{xstr => any_to_double(xstr.toDouble>10)}    
    labeledData = labeledData.withColumn("is_HR_OTS", is_HR_OTS(labeledData("orig_trans_st_filled_idx")))
    val not_HR_OTS = udf[Double, Double]{xstr => any_to_double(xstr.toDouble<=10)}    
    labeledData = labeledData.withColumn("not_HR_OTS", not_HR_OTS(labeledData("orig_trans_st_filled_idx")))
    
    println("is_HR_mcc")
    val HR_mccs = Array(284,189,217,259,203,263,262,33,48,258,21,67,4,255,81,133,195,13,36,158,89,5,23,61,124,207,257,47,199,7,93,194,169,0)
    val is_HR_mcc = udf[Double,Double]{xstr => any_to_double(HR_mccs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_mcc", is_HR_mcc(labeledData("mcc_cd_filled_idx")))
    val not_HR_mcc = udf[Double,Double]{xstr => any_to_double(!HR_mccs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_mcc", not_HR_mcc(labeledData("mcc_cd_filled_idx")))
     
    
    println("is_HR_IIC")
    val HR_IICs = Array(387,337,501,378,563,619,414,523,295,494,596,578,116,562,554,318,212,506,605,368,467,650,526,384,51,182,108,540,457,105,383,538,571,537,398,313,613,372,74,322,138,66,470,557,203,316,102,258,551,169,92,78,227,174,233,241,195,495,87,149,419,216,219,3,19,40,57)
    val is_HR_IIC = udf[Double, Double]{xstr => any_to_double(HR_IICs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_IIC", is_HR_IIC(labeledData("iss_ins_cd_filled_idx")))
    val not_HR_IIC = udf[Double, Double]{xstr => any_to_double(!HR_IICs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_IIC", not_HR_IIC(labeledData("iss_ins_cd_filled_idx")))
    
    
    println("is_HR_FSC")
    val HR_FSCs = Array(0,2,3,5,25,9,7,64,95,143,141,76,8,10,16,387,605,374,237,150,266,436,527,495,328,240,390,283,545,17,31)
    val is_HR_FSC = udf[Double, Double]{xstr => any_to_double(HR_FSCs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_FSC", is_HR_FSC(labeledData("fwd_settle_cruu_cd_filled_idx")))
    val not_HR_FSC = udf[Double, Double]{xstr => any_to_double(!HR_FSCs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_FSC", not_HR_FSC(labeledData("fwd_settle_cruu_cd_filled_idx")))
    
    
    println("is_HR_FSCR")
    val HR_FSCRs = Array(0,1,2,142,216,252,192,373,395,509,89,352,649,681,72,8,211,54,455,7,355,712,675,201,640,165,868,645,912,68,26,82,152,272,492,851,518,65,3,328,958,292,1146,441,236,736,467,755,925,524,540,939,180,580,149,52,119,389,372,774,125,244,106)
    val is_HR_FSCR = udf[Double, Double]{xstr => any_to_double(HR_FSCRs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_FSCR", is_HR_FSCR(labeledData("fwd_settle_conv_rt_filled_idx")))
    val not_HR_FSCR = udf[Double, Double]{xstr => any_to_double(!HR_FSCRs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_FSCR", not_HR_FSCR(labeledData("fwd_settle_conv_rt_filled_idx")))
    
    
    println("is_HR_CCC")
    val HR_CCCs = Array(0,1,3,2,15,48,60,121,79,122,355,202,340,129,217,246,490,564,509,220,4,461,352,299,22,403)
    val is_HR_CCC = udf[Double, Double]{xstr => any_to_double(HR_CCCs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_CCC", is_HR_CCC(labeledData("cdhd_curr_cd_filled_idx")))
    val not_HR_CCC = udf[Double, Double]{xstr => any_to_double(!HR_CCCs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_CCC", not_HR_CCC(labeledData("cdhd_curr_cd_filled_idx")))
    
    
    println("is_HR_CCR")
    val HR_CCRs = Array(0,1,3) 
    val is_HR_CCR = udf[Double, Double]{xstr => any_to_double(HR_CCRs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_CCR", is_HR_CCR(labeledData("cdhd_conv_rt_filled_idx")))
    val not_HR_CCR = udf[Double, Double]{xstr => any_to_double(!HR_CCRs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_CCR", not_HR_CCR(labeledData("cdhd_conv_rt_filled_idx")))
    
    println("is_HR_card_media")
    val HR_card_medias = Array(0,1,2,3,5,4,6,7,9) 
    val is_HR_card_media = udf[Double, Double]{xstr => any_to_double(HR_card_medias.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_card_media", is_HR_card_media(labeledData("card_media_cd_filled_idx")))
    val not_HR_card_media = udf[Double, Double]{xstr => any_to_double(!HR_card_medias.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_card_media", not_HR_card_media(labeledData("card_media_cd_filled_idx")))
    
     
    println("is_HR_CAC")
    val HR_CACs = Array(0,1,2,5,10,28,45,16,6,25,92,76,13,120,111,116,14) 
    val is_HR_CAC = udf[Double, Double]{xstr => any_to_double(HR_CACs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("is_HR_CAC", is_HR_CAC(labeledData("card_attr_cd_filled_idx")))
    val not_HR_CAC = udf[Double, Double]{xstr => any_to_double(!HR_CACs.contains(xstr.toInt))}    
    labeledData = labeledData.withColumn("not_HR_CAC", not_HR_CAC(labeledData("card_attr_cd_filled_idx")))
     
    labeledData.show(50)
    
    var lb_arr = Array("HR_CA","HR_AIRC","HR_MC","HR_Term","HR_CA_2","HR_AIRC_2","HR_MC_2","HR_Term_2","HR_TS","HR_Tccs","HR_Rc","HR_RScc","HR_RSCR","HR_pemc","HR_Pcc","HR_OTS","HR_mcc","HR_IIC","HR_FSC","HR_FSCR","HR_CCC","HR_CCR","HR_card_media","HR_CAC")
    var is_arr = Array(             "is_HR_AIRC","is_HR_MC","is_HR_Term","is_HR_CA_2","is_HR_AIRC_2","is_HR_MC_2","is_HR_Term_2","is_HR_TS","is_HR_Tccs","is_HR_Rc","is_HR_RScc","is_HR_RSCR","is_HR_pemc","is_HR_Pcc","is_HR_OTS","is_HR_mcc","is_HR_IIC","is_HR_FSC","is_HR_FSCR","is_HR_CCC","is_HR_CCR","is_HR_card_media","is_HR_CAC")
    var not_arr = Array("not_HR_CA","not_HR_AIRC","not_HR_MC","not_HR_Term","not_HR_CA_2","not_HR_AIRC_2","not_HR_MC_2","not_HR_Term_2","not_HR_TS","not_HR_Tccs","not_HR_Rc","not_HR_RScc","not_HR_RSCR","not_HR_pemc","not_HR_Pcc","not_HR_OTS","not_HR_mcc","not_HR_IIC","not_HR_FSC","not_HR_FSCR","not_HR_CCC","not_HR_CCR","not_HR_card_media","not_HR_CAC")

    var label_arr = is_arr ++ not_arr
    
    var countdf = labeledData.groupBy("certid").agg(sum("is_HR_CA") as "is_HR_CA_cnt") 
     
    println("start grouping...")
    
    
    for( item <- label_arr){
         val newcol = item + "_cnt"
         println(newcol)
         var tmpdf = labeledData.groupBy("certid").agg(sum(item) as newcol) 
         tmpdf.show(5)
         tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf(newcol))
         countdf = countdf.join(tmpdf, countdf("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
      }
    
    
    for( item <- lb_arr){
      val newcol = item + "_ratio"
      val iscol = "is_" + item + "_cnt"
      val notcol = "not_" + item + "_cnt"
      countdf = countdf.withColumn(newcol, countdf(iscol)/(countdf(iscol)+countdf(notcol)))
      
    }

    countdf = countdf.coalesce(1)
      
    countdf .show(20)
    var savepath = "xrli/credit/count_label_isnot.csv"
    val saveOptions = Map("header" -> "true", "path" -> savepath)
    countdf.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
        
        
  
     
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  

    
}