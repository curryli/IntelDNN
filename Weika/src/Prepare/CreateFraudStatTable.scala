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
//import org.apache.spark.mllib.stat.Statistics

import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.feature.ChiSqSelectorModel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions 

object CreateFraudStatTable {
    val startdate = IntelUtil.varUtil.startdate
    val enddate = IntelUtil.varUtil.enddate
    val rangedir = IntelUtil.varUtil.rangeDir 
    val usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
    val fraudType = "04"
 

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
           
    var fraud_join_Data = Fraud_16_new(ss, startdate, enddate).persist(StorageLevel.MEMORY_AND_DISK_SER)
         
    var fraudType_infraud = fraud_join_Data.filter(fraud_join_Data("fraud_tp")=== fraudType)  
    
    //var fraud_data = fraudType_infraud.selectExpr("sys_tra_no", "cast(pdate as double) as date", "pri_acct_no_conv", "term_id" , "trans_region", "mchnt_cd", "fraud_tp")
    var fraud_data = fraudType_infraud.selectExpr("sys_tra_no", "tfr_dt_tm", "pri_acct_no_conv", "term_id" , "trans_region", "mchnt_cd", "fraud_tp")
 
    val getdate = udf[Long, String] { xstr => xstr.reverse.substring(6).reverse.toLong }
    fraud_data = fraud_data.withColumn("date", getdate(fraud_data("tfr_dt_tm")))
    fraud_data.show
    
    
    val wd_term = Window.partitionBy("term_id").orderBy("date")
     //该笔交易的term3天内的欺诈交易数
    val W_term_day3 = wd_term.rangeBetween(-3, -1)  //累加前3天,当日除外
    fraud_data = fraud_data.withColumn("day3_fcnt_term", count("sys_tra_no").over(W_term_day3))
     //该笔交易的term7天内的欺诈交易数
    val W_term_day7 = wd_term.rangeBetween(-7, -1)  //累加前7天,当日除外
    fraud_data = fraud_data.withColumn("day7_fcnt_term", count("sys_tra_no").over(W_term_day7))
     //该笔交易的term30天内的欺诈交易数
    val W_term_day30 = wd_term.rangeBetween(-30, -1)  //累加前30天,当日除外
    fraud_data = fraud_data.withColumn("day30_fcnt_term", count("sys_tra_no").over(W_term_day30))
    
      
    
    
    val wd_mchnt = Window.partitionBy("mchnt_cd").orderBy("date")
    //该笔交易的商户3天内的欺诈交易数
    val W_mchnt_day3 = wd_mchnt.rangeBetween(-3, -1)  //累加前3天,当日除外
    fraud_data = fraud_data.withColumn("day3_fcnt_mchnt", count("sys_tra_no").over(W_mchnt_day3))
    //该笔交易的商户7天内的欺诈交易数
    val W_mchnt_day7 = wd_mchnt.rangeBetween(-7, -1)  //累加前7天,当日除外
    fraud_data = fraud_data.withColumn("day7_fcnt_mchnt", count("sys_tra_no").over(W_mchnt_day7))
    //该笔交易的商户30天内的欺诈交易数
    val W_mchnt_day30 = wd_mchnt.rangeBetween(-30, -1)  //累加前30天,当日除外
    fraud_data = fraud_data.withColumn("day30_fcnt_mchnt", count("sys_tra_no").over(W_mchnt_day30))
    
    
    println(fraud_data.columns.mkString(","))
    //sys_tra_no,tfr_dt_tm,pri_acct_no_conv,term_id,trans_region,mchnt_cd,fraud_tp,date,day3_fcnt_term,day7_fcnt_term,day30_fcnt_term,day3_fcnt_mchnt,day7_fcnt_mchnt,day30_fcnt_mchnt
    
    fraud_data.write.csv("xrli/IntelDNN/Weika/weika_term_mchnt_stat")
    
//    fraud_data.registerTempTable("fraud_data_TB")  
//    sql("create hive table weika_term_mchnt_stat as select * from fraud_data_TB")
    
    
    //fraud_data.registerTempTable("fraud_data_TB")
     
    //前三天该卡消费的不同term数
  //ss.sql("select pri_acct_no_conv, count(distinct term_id_select) as term_cnt from (SELECT pri_acct_no_conv, first(term_id) over(partition by pri_acct_no_conv, term_id order by date range between 3 preceding and 1 following) as term_id_select from fraud_data_TB)tmp group by pri_acct_no_conv").show  
   

    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
   
    def Fraud_16_new(ss: SparkSession, startdate:String, enddate:String):DataFrame = {
    		val sc = ss.sparkContext
    	  val filename = "xrli/IntelDNN/Fraud_16_new"
		    val fraud_join_Rdd = sc.textFile(filename).map(str=> str.split("\\001")).filter(tmparr=> tmparr(1)>=startdate && tmparr(1)<=enddate).map{ tmparr=>
			    Row.fromSeq(tmparr.toSeq)
    		}
		    val schema_fraud_join = StructType(StructField("sys_tra_no",StringType,true)::StructField("trans_dt",StringType,true)::StructField("tfr_dt_tm",StringType,true)::StructField("pri_acct_no_conv",StringType,true)::StructField("term_id",StringType,true)::StructField("trans_region",StringType,true)::StructField("mchnt_cd",StringType,true)::StructField("fraud_tp",StringType,true)::Nil)
			  val fraud_join_DF = ss.createDataFrame(fraud_join_Rdd, schema_fraud_join) 
			  fraud_join_DF
     }
    
    
}



    

//insert overwrite directory 'xrli/IntelDNN/Fraud_16_new'
//select * from(  
//select sys_tra_no,trans_dt, tfr_dt_tm, ar_pri_acct_no, term_id , trans_region, mchnt_cd, fraud_tp
//from tbl_arsvc_fraud_trans
//where trans_dt>='20160101' and trans_dt<='20161231'
//)tmp1;