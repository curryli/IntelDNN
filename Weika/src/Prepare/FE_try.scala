package Prepare
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }
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
import scala.collection.mutable.{ Buffer, Set, Map }
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
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorAssembler }
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

import org.apache.spark.sql.expressions._

import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date

object FE_try {

  val startdate = IntelUtil.varUtil.startdate
  val enddate = IntelUtil.varUtil.enddate
  val rangedir = IntelUtil.varUtil.rangeDir

  var idx_modelname = IntelUtil.varUtil.idx_model

  def bool_2_string(b: Boolean): String = {
    if (b == true)
      "1"
    else
      "0"
  }

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
    var labeledData = IntelUtil.get_from_HDFS.get_labeled_DF(ss, input_dir).persist(StorageLevel.MEMORY_AND_DISK_SER) // .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//

    //labeledData = labeledData.sample(false, 0.001)
    
    println("labeledData obtained")
    //labeledData = labeledData.sort("pri_acct_no_conv", "tfr_dt_tm")   //只能同时升序或者降序，否咋需要用二次排序

     //统计卡最常用交易地点
    
//    val most_frequent_locs_DF2 = labeledData.selectExpr("acpt_ins_id_cd_RG, count(acpt_ins_id_cd_RG) as cnt group by pri_acct_no_conv order by cnt")
//    most_frequent_locs_DF2.show
    
    labeledData.registerTempTable("labeledData_TB")
    //labeledData.select("pri_acct_no_conv", "acpt_ins_id_cd_RG").registerTempTable("card_locs_tb")
    //var results = ss.sql("SELECT pri_acct_no_conv, count(acpt_ins_id_cd_RG) as cnt FROM card_locs_tb GROUP BY pri_acct_no_conv")  
    //results = ss.sql("SELECT * from (SELECT pri_acct_no_conv, acpt_ins_id_cd_RG, count(*) as cnt FROM card_locs_tb GROUP BY pri_acct_no_conv, acpt_ins_id_cd_RG)tmp ORDER BY pri_acct_no_conv, cnt")
    //results = ss.sql("SELECT MAX(cnt) as cnt2 from (SELECT pri_acct_no_conv, acpt_ins_id_cd_RG, count(*) as cnt FROM card_locs_tb GROUP BY pri_acct_no_conv, acpt_ins_id_cd_RG)tmp GROUP BY pri_acct_no_conv")

    
    //统计卡最常用交易地点
    val most_frequent_locs_DF = ss.sql("SELECT pri_acct_no_conv, FIRST(acpt_ins_id_cd_RG) AS mlocs, MAX(cnt) AS mcnt from (SELECT pri_acct_no_conv, acpt_ins_id_cd_RG, count(*) as cnt FROM labeledData_TB GROUP BY pri_acct_no_conv, acpt_ins_id_cd_RG)tmp GROUP BY pri_acct_no_conv ORDER BY mcnt DESC")
    most_frequent_locs_DF.show
    
    //统计卡最常用交易省
    val most_frequent_provs_DF = ss.sql("SELECT pri_acct_no_conv, FIRST(prov) AS mprovs, MAX(cnt) AS mcnt from (SELECT pri_acct_no_conv, substring(acpt_ins_id_cd_RG,0,2) as prov, count(*) as cnt FROM labeledData_TB GROUP BY pri_acct_no_conv, substring(acpt_ins_id_cd_RG,0,2))tmp GROUP BY pri_acct_no_conv ORDER BY mcnt DESC")
    most_frequent_provs_DF.show
 
    
    labeledData = labeledData.join(most_frequent_locs_DF, labeledData("pri_acct_no_conv")===most_frequent_locs_DF("pri_acct_no_conv"), "left_outer").drop(labeledData("pri_acct_no_conv"))
    labeledData = labeledData.join(most_frequent_provs_DF, labeledData("pri_acct_no_conv")===most_frequent_provs_DF("pri_acct_no_conv"), "left_outer").drop(labeledData("pri_acct_no_conv"))
  
    
    
    println("All done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")
  }

}