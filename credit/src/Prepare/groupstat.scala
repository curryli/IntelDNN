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
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.feature.QuantileDiscretizer


object groupstat {
 
    val replace_Not_num = udf[Double, Double]{xstr => 
    var result = 0.0
    try{
        result = xstr.toDouble
     }
     catch{
       case ex: java.lang.NumberFormatException => {result = -1}
     }
            
      result
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
 
    val train_ori_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/train_trans_encrypt.csv")
    val test_ori_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/test_trans_encrypt.csv")
     
    var labeledData =  train_ori_df.unionAll(test_ori_df)
    labeledData = labeledData.na.fill(-1)
      
    labeledData.dtypes.foreach(println)
    
    
//    var countdf = labeledData.groupBy("certid").agg(count("Settle_dt") as "trans_cnt") 
//
//    val depositdf = labeledData.filter(labeledData("card_media_cd")==="01")
//    var tmpdf = depositdf.groupBy("certid").agg(countDistinct("card_no") as "deposit_cards_cnt") 
//    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("deposit_cards_cnt"))
//    countdf = countdf.join(tmpdf, countdf("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
//    
//    val creditdf = labeledData.filter(labeledData("card_media_cd")!=="01") 
//    tmpdf = creditdf.groupBy("certid").agg(countDistinct("card_no") as "credit_cards_cnt") 
//    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("credit_cards_cnt"))
//    countdf = countdf.join(tmpdf, countdf("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
//    
//    countdf = countdf.withColumn("credit_cards_ratio", countdf("credit_cards_cnt")/(countdf("credit_cards_cnt")+countdf("deposit_cards_cnt")))
//       
//    
//    tmpdf = depositdf.groupBy("certid").agg(count("Settle_dt") as "deposit_trans_cnt") 
//    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("deposit_trans_cnt"))
//    countdf = countdf.join(tmpdf, countdf("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
//    
//    tmpdf = creditdf.groupBy("certid").agg(count("Settle_dt") as "credit_trans_cnt") 
//    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("credit_trans_cnt"))
//    countdf = countdf.join(tmpdf, countdf("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
//    countdf = countdf.withColumn("credit_trans_ratio", countdf("credit_trans_cnt")/(countdf("credit_trans_cnt")+countdf("deposit_trans_cnt")))
//    
//   
//    labeledData = labeledData.withColumn("Trans_money", replace_Not_num(labeledData("Trans_at")))
//     
//     
//    //获取交易金额 （元）
//    println("RMB")
//    val getRMB = udf[Double, Double]{xstr => (xstr.toDouble/100)}
//    labeledData = labeledData.withColumn("RMB", getRMB(labeledData("Trans_money")))
//    
//    labeledData = labeledData.drop("Trans_money")
//    
//    val QD_N = 10
//    labeledData = discretizerFun("RMB", QD_N).fit(labeledData).transform(labeledData)	
//    
//    for(i <- 0 to QD_N){
//      val newcol = "RMB_" + i.toString() + "_cnt"
//      println(newcol)
//      val RMB_filter = labeledData.filter(labeledData("RMB_QD")===i)
//      var tmpdf = RMB_filter.groupBy("certid").agg(count("RMB_QD") as newcol) 
//      tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf(newcol))
//      countdf = countdf.join(tmpdf, countdf("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
//    }
//    
//    
//    
//    
//   val totalMoney_splits = Array(0,10,100,1000,10000,Double.PositiveInfinity)
//
//	 val totalMoney_bucketizer = new Bucketizer()
//          .setInputCol("RMB")
//          .setOutputCol("RMB_BK")
//          .setSplits(totalMoney_splits)     
//	  
//	    
//	 labeledData = totalMoney_bucketizer.transform(labeledData)
//    
//   for(i <- 0 to totalMoney_splits.length){
//      val newcol = "RMB_bk_" + i.toString() + "_cnt"
//      println(newcol)
//      val RMB_filter = labeledData.filter(labeledData("RMB_BK")===i)
//      var tmpdf = RMB_filter.groupBy("certid").agg(count("RMB_BK") as newcol) 
//      tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf(newcol))
//      countdf = countdf.join(tmpdf, countdf("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
//    } 
//    
//   
//   var label_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/train_label_encrypt.csv")
//    
//   label_df = label_df.select(label_df("certid").as("certid_label"), label_df("label"))
//   countdf = countdf.join(label_df, countdf("certid")===label_df("certid_label"), "left_outer").drop("certid_label")
//   
//   
//   
//   
//   
//   
//    println("risk_resp_cd")
//    val HR_resp_cds = Array("34","38","51","55","57","58","65","75","93") 
//    val risk_resp_cd = udf[Double, String]{xstr => any_to_double(HR_resp_cds.contains(xstr))}    
//    labeledData = labeledData.withColumn("risk_resp_cd", risk_resp_cd(labeledData("resp_cd")))
//    tmpdf = labeledData.groupBy("certid").agg(sum("risk_resp_cd") as "risk_resp_cd_cnt") 
//    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("risk_resp_cd_cnt"))
//    countdf = countdf.join(tmpdf, countdf("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
//   
//   
//    println("fenqi")
//    val HR_trans_ids = Array("S13","V13","D13","D32","R13","R17","S84") 
//    val fenqi = udf[Double, String]{xstr => any_to_double(HR_trans_ids.contains(xstr))}    
//    labeledData = labeledData.withColumn("fenqi", fenqi(labeledData("trans_id_cd")))
//    tmpdf = labeledData.groupBy("certid").agg(sum("fenqi") as "fenqi_cnt") 
//    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("fenqi_cnt"))
//    countdf = countdf.join(tmpdf, countdf("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
//   
//   
//    val CX_arr = Array("S00","S01") 
//    val chaxun = udf[Double, String]{xstr => any_to_double(CX_arr.contains(xstr))}    
//    labeledData = labeledData.withColumn("chaxun", chaxun(labeledData("trans_id_cd")))
//    tmpdf = labeledData.groupBy("certid").agg(sum("chaxun") as "chaxun_cnt") 
//    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("chaxun_cnt"))
//    countdf = countdf.join(tmpdf, countdf("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
//   
//    val quxian = udf[Double, String]{xstr => any_to_double(xstr=="S24")}    
//    labeledData = labeledData.withColumn("quxian", quxian(labeledData("trans_id_cd")))
//    tmpdf = labeledData.groupBy("certid").agg(sum("quxian") as "quxian_cnt") 
//    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("quxian_cnt"))
//    countdf = countdf.join(tmpdf, countdf("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
//   
//    val transfer_arr = Array("S33","S25","S26") 
//    val transfer = udf[Double, String]{xstr => any_to_double(transfer_arr.contains(xstr))}    
//    labeledData = labeledData.withColumn("transfer", transfer(labeledData("trans_id_cd")))
//    tmpdf = labeledData.groupBy("certid").agg(sum("transfer") as "transfer_cnt") 
//    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("transfer_cnt"))
//    countdf = countdf.join(tmpdf, countdf("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
//    
//   countdf = countdf.na.fill(0)
//
//    
//   println(countdf.columns.mkString(","))
//        
//   countdf.rdd.map(_.mkString(",")).saveAsTextFile("xrli/credit/groupstat_2.csv")
   

    println("is_risk_mcc")
    val risk_mccs = Array("5912","7297","7298","4835")
    val high_risk_mccs = Array("5976","7995","4835","5933","9211","9222","9223")
    
    val gg_mccs = Array("4900")
    val zb_mccs = Array("5094","5944","5950","5970","5971")
    val qc_mccs = Array("5271","5511","5521","5532","5533","5541","5542","5551","5561","5571","5592","5598","5599","5983")
    val zx_mccs = Array("5211","5231","5251","5261","5712","5713","5714","5718","5719","5992")
    val zs_mccs = Array("7011","7012")
    val xx_mccs = Array("7032","7033","7297","7298","7829","7832","7911","7922","7929","7932","7933","7941","7991","4733","7992","7994","7995","7996","7997","7998","7999","4835","5018")
    val fdc_mccs = Array("1520","7013")
    val jr_mccs = Array("5933","6051","6211","6300","6010","6011","6012","9498","6013","6050","8888","9991","5690","6014")
    val jm_mccs = Array("4900","7210","7211","7216","7217","7221","7230","7261","7273","7295","7395","7523","7299")
    val sy_mccs = Array("763","780","4722","5811","5935","7276","7277","7278","7311","7321","7333","7338","7339","7361","7392","7393","7549","8111","8675","8931","7399","8911","8912","9988")
    val cz_mccs = Array("4457","4468","7296","7394","7512","7513","7519","7841")
    val jt_mccs = Array("3998","4111","4112","4119","4121","4131","4411","4511","4582","4784","4789")
    val wl_mccs = Array("4011","4214","4215","4225","9402")
    val jsj_mccs = Array("4814","4816","4821","7372","7375","4899","3333","4958")
    val fw_mccs = Array("742","6513","7251","7342","7349","7379","7531","7534","7535","7538","7542","7622","7623","7629","7631","7641","7692","7699","8999")
    val jy_mccs = Array("8211","8220","8241","8244","8249","8299","8351","8229")
    val ws_mccs = Array("8011","8021","8031","8041","8042","8049","8050","8062","8071","8099")
    val sh_mccs = Array("8641","8651","8661","8699")
    val zf_mccs = Array("8398","9211","9222","9223","9311","9400","9399","9708","4802","5317","8399")
    val pf_mccs = Array("5013","5021","5039","5044","5045","5046","5047","5051","5065","5072","5074","5111","5122","5131","5137","5139","5172","5192","5193","5198","5398","4458","5998","9705","5531")
    val zhix_mccs = Array("5960","5962","5963","5964","5965","5966","5967","5968","5969")
    
 
    val risk_mcc = udf[Double, String]{xstr => any_to_double(risk_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_risk_mcc", risk_mcc(labeledData("mcc_cd")))
    
    var count_mcc_df = labeledData.groupBy("certid").agg(sum("is_risk_mcc") as "risk_mcc_cnt") 
        
        
    
    val high_risk_mcc = udf[Double, String]{xstr => any_to_double(high_risk_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_high_risk_mcc", high_risk_mcc(labeledData("mcc_cd"))) 
    var tmpdf = labeledData.groupBy("certid").agg(sum("is_high_risk_mcc") as "high_risk_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("high_risk_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    
    
    val gg_mcc = udf[Double, String]{xstr => any_to_double(gg_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_gg_mcc", gg_mcc(labeledData("mcc_cd")))
    tmpdf = labeledData.groupBy("certid").agg(sum("is_gg_mcc") as "gg_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("gg_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    
    val zb_mcc = udf[Double, String]{xstr => any_to_double(zb_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_zb_mcc", zb_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_zb_mcc") as "zb_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("zb_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val qc_mcc = udf[Double, String]{xstr => any_to_double(qc_mccs .contains(xstr))}    
    labeledData = labeledData.withColumn("is_qc_mcc", qc_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_qc_mcc") as "qc_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("qc_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val zs_mcc = udf[Double, String]{xstr => any_to_double(zs_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_zs_mcc", zs_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_zs_mcc") as "zs_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("zs_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
 
    val xx_mcc = udf[Double, String]{xstr => any_to_double(xx_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_xx_mcc", xx_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_xx_mcc") as "xx_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("xx_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val fdc_mcc = udf[Double, String]{xstr => any_to_double(fdc_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_fdc_mcc", fdc_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_fdc_mcc") as "fdc_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("fdc_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val jr_mcc = udf[Double, String]{xstr => any_to_double(jr_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_jr_mcc", jr_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_jr_mcc") as "jr_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("jr_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val jm_mcc = udf[Double, String]{xstr => any_to_double(jm_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_jm_mcc", jm_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_jm_mcc") as "jm_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("jm_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val sy_mcc = udf[Double, String]{xstr => any_to_double(sy_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_sy_mcc", sy_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_sy_mcc") as "sy_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("sy_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val cz_mcc = udf[Double, String]{xstr => any_to_double(cz_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_cz_mcc", cz_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_cz_mcc") as "cz_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("cz_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val jt_mcc = udf[Double, String]{xstr => any_to_double(jt_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_jt_mcc", jt_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_jt_mcc") as "jt_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("jt_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val wl_mcc = udf[Double, String]{xstr => any_to_double(wl_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_wl_mcc", wl_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_wl_mcc") as "wl_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("wl_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val jsj_mcc = udf[Double, String]{xstr => any_to_double(jsj_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_jsj_mcc", jsj_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_jsj_mcc") as "jsj_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("jsj_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val fw_mcc = udf[Double, String]{xstr => any_to_double(fw_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_fw_mcc", fw_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_fw_mcc") as "fw_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("fw_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val jy_mcc = udf[Double, String]{xstr => any_to_double(jy_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_jy_mcc", jy_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_jy_mcc") as "jy_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("jy_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val ws_mcc = udf[Double, String]{xstr => any_to_double(ws_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_ws_mcc", ws_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_ws_mcc") as "ws_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("ws_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val sh_mcc = udf[Double, String]{xstr => any_to_double(sh_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_sh_mcc", sh_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_sh_mcc") as "sh_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("sh_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val zf_mcc = udf[Double, String]{xstr => any_to_double(zf_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_zf_mcc", zf_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_zf_mcc") as "zf_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("zf_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val pf_mcc = udf[Double, String]{xstr => any_to_double(pf_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_pf_mcc", pf_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_pf_mcc") as "pf_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("pf_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    val zhix_mcc = udf[Double, String]{xstr => any_to_double(zhix_mccs.contains(xstr))}    
    labeledData = labeledData.withColumn("is_zhix_mcc", zhix_mcc(labeledData("mcc_cd")))
        tmpdf = labeledData.groupBy("certid").agg(sum("is_zhix_mcc") as "zhix_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("zhix_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
    
    var label_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/train_label_encrypt.csv")
    label_df = label_df.select(label_df("certid").as("certid_label"), label_df("label"))
    
    count_mcc_df = count_mcc_df.join(label_df, count_mcc_df("certid")===label_df("certid_label"), "left_outer").drop("certid_label")
    
    
    count_mcc_df = count_mcc_df.na.fill(-1)
  
    
   count_mcc_df = count_mcc_df.coalesce(1)
   var savepath = "xrli/credit/groupMCC.csv"
   val saveOptions = Map("header" -> "true", "path" -> savepath)
   count_mcc_df.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    
   ///////////////////////////crossTB////////////////////////////////
   val Arr_dist = Array("risk_mcc_cnt","high_risk_mcc_cnt","gg_mcc_cnt","zb_mcc_cnt","qc_mcc_cnt","zs_mcc_cnt","xx_mcc_cnt","fdc_mcc_cnt","jr_mcc_cnt","jm_mcc_cnt","sy_mcc_cnt","label","cz_mcc_cnt","jt_mcc_cnt","wl_mcc_cnt","jsj_mcc_cnt","fw_mcc_cnt","jy_mcc_cnt","ws_mcc_cnt","sh_mcc_cnt","zf_mcc_cnt","pf_mcc_cnt","zhix_mcc_cnt")
 
   for(col <- Arr_dist){
        var tempdf = count_mcc_df.stat.crosstab(col, "label")
        tempdf = tempdf.withColumn("ratio", tempdf("1")/tempdf("0"))
        tempdf = tempdf.coalesce(1)
        var savepath = "xrli/credit/train_cross_MCC/" + col + ".csv"
        val saveOptions = Map("header" -> "true", "path" -> savepath)
        tempdf.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
  }
    
    
    
    
}
  
   
  
    def discretizerFun (col: String, bucketNo: Int): QuantileDiscretizer = {
       val discretizer = new QuantileDiscretizer()
         discretizer.setInputCol(col)
                    .setOutputCol(s"${col}_QD")
                    .setNumBuckets(bucketNo)
   }
    
    
  def any_to_double[T: ClassTag](b: T):Double={
    if(b==true)
      1.0
    else
      0
  }
  
}