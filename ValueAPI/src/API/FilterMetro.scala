package API

import scala.math._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
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
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification._
import org.apache.spark.storage.StorageLevel
import scala.reflect.ClassTag
 

object FilterMetro { 
  def main(args: Array[String]) { 
     //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    val ss = SparkSession.builder().appName("DataSet basic example").appName("Dataset example").getOrCreate()
 
   // For implicit conversions like converting RDDs to DataFrames
    import ss.implicits._
    val sc = ss.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
 
    //读文件
    val metro_related_data = ss.read.text("xrli/CardholderTag/metro_related_Data2").as[String]
 
    val metro_relatedRDD = metro_related_data.map(line =>
       if(line.split(',').length==5)
        (line.split(',')(0), line.split(',')(1).toDouble, line.split(',')(2), line.split(',')(3), line.split(',')(4))
       else
        (line.split(',')(0), 0.0, "", "", "")
    )
     
    var metro_related_DF = metro_relatedRDD.toDF("card", "trans_at", "tfr_dt_tm","ext_extend_inf_28", "mchnt_tp").persist(StorageLevel.MEMORY_AND_DISK_SER) 
      
    
    println("metro_related_DF distribute: ")
    metro_related_DF.createOrReplaceTempView("metro_related_TB")
    sqlContext.sql("select pay_tp,count(*) from (select ext_extend_inf_28 as pay_tp from metro_related_TB where ext_extend_inf_28>='1' and ext_extend_inf_28<='9')A group by pay_tp").show
    
    
    
    val is_QR = udf[Double, String]{xstr => 
      if(xstr.length()<1)
        0.0
      else{
        var a = xstr.charAt(0)  //最后一位
       if(a>='1' & a<='9')  //
          1.0
        else
          0.0
      }
    }
    
    metro_related_DF = metro_related_DF.withColumn("is_QR", is_QR(metro_related_DF("ext_extend_inf_28")))
    
    val All_SF_DF = metro_related_DF.filter(metro_related_DF("is_QR").===(1.0))
    
    //////////////////////////////////////////
    val metro_stat_data = ss.read.text("xrli/CardholderTag/ValueAPI_Metro2").as[String]
 
    val metro_statRDD = metro_stat_data.map(line => (line.split(',')(0), line.split(',')(1).toInt, line.split(',')(2).toDouble, line.split(',')(3).toDouble,
     line.split(',')(4).toInt, line.split(',')(5).toInt))
     
    val metro_stat_DF = metro_statRDD.toDF("card", "QR_cnt", "QR_cnt_ratio","QR_avg_RMB", "QR_date_cnt", "QR_mcc_tps").persist(StorageLevel.MEMORY_AND_DISK_SER) 
     
    //val Metro_valuable_stat = metro_stat_DF.filter(metro_stat_DF("QR_cnt")>=15 && metro_stat_DF("QR_cnt_ratio")>=0.5 && metro_stat_DF("QR_avg_RMB")>=100 && metro_stat_DF("QR_date_cnt")>=10 && metro_stat_DF("QR_mcc_tps")>=3).persist(StorageLevel.MEMORY_AND_DISK_SER) 
    val Metro_valuable_stat = metro_stat_DF.filter(metro_stat_DF("QR_cnt")>=1 && metro_stat_DF("QR_cnt_ratio")>=0.5 && metro_stat_DF("QR_avg_RMB")>=100 && metro_stat_DF("QR_date_cnt")>=1 && metro_stat_DF("QR_mcc_tps")>=3).persist(StorageLevel.MEMORY_AND_DISK_SER) 
      
    
    //所有卡号个数
    val all_cards_cnt = metro_related_DF.select("card").distinct().count()
    
    //所有优质卡号个数
    val valueable_cards_cnt = Metro_valuable_stat.count()
    
    println("all_cards_cnt : " + all_cards_cnt + "  valueable_cards_cnt: " + valueable_cards_cnt)
    
    //所有优质卡号的交易
    val all_valuable_DF = metro_related_DF.join(Metro_valuable_stat, Seq("card"), "leftsemi")
    
    //所有优质卡号的闪付交易
    val all_val_SF_DF = All_SF_DF.join(Metro_valuable_stat, Seq("card"), "leftsemi")

    
    //所有优质卡号的交易的笔数
    val valuable_transcnt = all_valuable_DF.count()
    
    //所有卡号交易笔数
    val all_transcnt = metro_related_DF.count
    
    //所有优质卡号的闪付交易笔数
    val valuable_SF_transcnt = all_val_SF_DF.count()
    
    //所有闪付交易笔数
    val all_SF_transcnt = All_SF_DF.count
     
    
    println("valuable_transcnt: " + valuable_transcnt + "  all_transcnt: " + all_transcnt + "  valuable_SF_transcnt: " + valuable_SF_transcnt + "  all_SF_transcnt: " + all_SF_transcnt)
    
    
    //所有优质卡号的闪付交易的金额
    println("all_val_SF_DF RMB sum: ")
    all_val_SF_DF.agg(sum("trans_at")/100).show 
     
    //所有闪付交易的金额
    println("All_SF_DF RMB sum: ")
    All_SF_DF.agg(sum("trans_at")/100).show 
 
    //所有闪付交易中，每种闪付交易的类型分布
    println("All_SF_DF distribute: ")
    All_SF_DF.createOrReplaceTempView("All_SF_TB")
    sqlContext.sql("select pay_tp,count(*) from (select ext_extend_inf_28 as pay_tp from All_SF_TB where ext_extend_inf_28>='1' and ext_extend_inf_28<='9')A group by pay_tp").show
    
    
    //所有优质卡号的闪付交易中，每种闪付交易的类型分布
    println("all_val_SF_DF distribute:")
    all_val_SF_DF.createOrReplaceTempView("all_val_SF_TB")
    sqlContext.sql("select pay_tp,count(*) from (select ext_extend_inf_28 as pay_tp from all_val_SF_TB where ext_extend_inf_28>='1' and ext_extend_inf_28<='9')A group by pay_tp").show
    
    
  }
} 