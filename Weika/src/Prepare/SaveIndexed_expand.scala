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

import org.apache.spark.sql.expressions._

import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date

object SaveIndexed_expand {
 
  var idx_modelname = IntelUtil.varUtil.idx_model

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
    var labeledData = IntelUtil.get_from_HDFS.get_labeled_DF(ss, input_dir).persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    //labeledData.show(10)
    
    //去除借记卡 
    labeledData = labeledData.filter(labeledData("card_attr").=!=("01") )
     
    
    //817214312  817    1117214312  1117 
    val getdate = udf[Long, String]{xstr => xstr.reverse.substring(6).reverse.toLong}
    
    labeledData = labeledData.withColumn("date", getdate(labeledData("tfr_dt_tm")))
 
    //一天内总交易金额
    val total_money_day_DF = labeledData.groupBy("pri_acct_no_conv","date").agg(sum("trans_at") as "total_money_day")//.toDF("total_money_day") 
    labeledData = labeledData.join(total_money_day_DF, (labeledData("pri_acct_no_conv")===total_money_day_DF("pri_acct_no_conv") &&  labeledData("date")===total_money_day_DF("date")), "left_outer").drop(labeledData("pri_acct_no_conv")).drop(labeledData("date"))
    labeledData.show(5)
    
    //一天内总交易次数
    val total_times_day_DF = labeledData.groupBy("pri_acct_no_conv","date").agg(count("trans_at") as "total_times_day") 
    labeledData = labeledData.join(total_times_day_DF, (labeledData("pri_acct_no_conv")===total_times_day_DF("pri_acct_no_conv") &&  labeledData("date")===total_times_day_DF("date")), "left_outer").drop(labeledData("pri_acct_no_conv")).drop(labeledData("date"))
    labeledData.show(5)
    
    labeledData = labeledData.sort("pri_acct_no_conv", "tfr_dt_tm")   //只能同时升序或者降序，否咋需要用二次排序
    labeledData.show(100)
    
    
    //////window操作///////////////// http://www.mamicode.com/info-detail-1366095.html          http://www.cnblogs.com/kevingu/p/5140242.html   http://www.mamicode.com/info-detail-1366095.html
   
        
    //println("select sql")  //没有注册tablelabeledData
    //sql("select pri_acct_no_conv,tfr_dt_tm,sum(trans_at) over (partition by pri_acct_no_conv order by tfr_dt_tm asc) as sum_duration from labeledData").show()
    

    val wt = Window.partitionBy("pri_acct_no_conv").orderBy("tfr_dt_tm")

    //labeledData = labeledData.withColumn("row_trans", functions.row_number().over(wt))
    val rowW_1t = wt.rowsBetween(-1, 0)  //累加到前1笔
    labeledData = labeledData.withColumn("amount_1trans", sum("trans_at").over(rowW_1t)) 
    println("rowW_1t")
    labeledData.show(50)
     
    val rowW_3t = wt.rowsBetween(-3, 0)  //累加到前3笔
    labeledData = labeledData.withColumn("amount_3trans", sum("trans_at").over(rowW_3t)) 
    println("rowW_3t")
    labeledData.show(50)
    

          
//rows表示 行，就是前n行，后n行
//range表示的是 具体的值，比这个值小n的行，比这个值大n的行   要用rangeBetween，orderBy( )里面的列必须是数值型的
    
    val wd = Window.partitionBy("pri_acct_no_conv").orderBy("date")
    val rangeW_1 = wd.rangeBetween(-1, 0)    //累加到前1天
    labeledData = labeledData.withColumn("amount_1days", sum("trans_at").over(rangeW_1)) 
    println("rangeW_1")
    labeledData.show(50)
     
    val rangeW_3 = wd.rangeBetween(-3, 0)  //累加到前3天
    labeledData = labeledData.withColumn("amount_3days", sum("trans_at").over(rangeW_3)) 
    println("rangeW_3")
    labeledData.show(50)
    
    
    
        
//    println("select expr")
//    labeledData.select(
//      $"pri_acct_no_conv",
//      $"tfr_dt_tm",
//      sum("trans_at").over(wd).as("sum_amount")
//    ).show(50)
    
 
//    labeledData = labeledData.withColumn("last_money_1", functions.lag("trans_at", 1).over(wt))
//    labeledData = labeledData.withColumn("delta_money_1", labeledData("trans_at")-labeledData("last_money_1"))
    
     //当笔交易和上一笔交易的金额差
    labeledData = labeledData.withColumn("delta_money_1", labeledData("trans_at")-functions.lag("trans_at", 1).over(wt))
    labeledData.show(50)
    
    
    
    //import org.apache.spark.sql.catalyst.expressions.DateDiff
    labeledData = labeledData.withColumn("last_time_1", functions.lag("tfr_dt_tm", 1).over(wt))
    
    
    val format = new SimpleDateFormat("MMddHHmmss")
    
    //val formatTime = udf[java.util.Date, String]{xstr => format.parse(xstr)}
    //val formatTime = udf[Long, String]{xstr => format.parse(xstr).getTime()}   //注意可能会有上一次时间为null的，就转换不了成时间了
    
    val formatTime = udf[Double, String]{xstr => 
      if(xstr!=null)
        format.parse(xstr).getTime().toDouble
      else
        Double.NaN
      }   //注意可能会有上一次时间为Null的，就转换不了成时间了

    val getInt = udf[Double, Double]{xstr => math.ceil(xstr.toFloat)}
    
    labeledData = labeledData.withColumn("delta_minutes_1", getInt((formatTime(labeledData("tfr_dt_tm")) - formatTime(functions.lag("tfr_dt_tm", 1).over(wt)))/(1000*60)) )
     
    labeledData.show(50)
    
    
    
    
    
    
    
    
    
    
    
    
////////////////////////////////
//     
//    val my_index_Model = PipelineModel.load(idx_modelname)
//    println("Load pipeline done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
//     
//    println("start transform data!")
//    labeledData = my_index_Model.transform(labeledData)
//    println("Indexed done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
//    labeledData.show(5)
/////////////////////////////////    
//     
//    
//    println(labeledData.columns.mkString(","))
//  
//    //labeledData.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "idx_withlabel_all")
//    
//    labeledData.selectExpr(IntelUtil.constUtil.delArr:_*).rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "idx_withlabel_del")
//
//    //labeledData.rdd.take(5).map(_.mkString(",")).foreach { println } 
    
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  

    
}