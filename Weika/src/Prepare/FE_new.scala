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
import scala.reflect.ClassTag

object FE_new {
  
  val rangedir = IntelUtil.varUtil.rangeDir 
    
  def any_to_double[T: ClassTag](b: T):Double={
    if(b==true)
      1.0
    else
      0
  }
  
   
  val udf_bool_to_double = udf[Double, Boolean]{xstr => any_to_double(xstr)} 
  val udf_int_to_double = udf[Double, Int]{xstr => any_to_double(xstr)}  
  val getdate = udf[Long, String]{xstr => xstr.substring(0,4).toLong}
  val get_day_week = udf[Int, String]{xstr => IntelUtil.funUtil.dayForWeek("2016" + xstr.substring(0,4)).toInt}
  
  
  def FE_function(ss: SparkSession, inputData: DataFrame):DataFrame = {
    //  获取月+日     817214312  817    1117214312  1117 
    var labeledData = inputData
    //val getdate = udf[Long, String]{xstr => xstr.reverse.substring(6).reverse.toLong}
    
    //labeledData = labeledData.sample(false, 0.0001).cache
    
    
     
    labeledData = labeledData.withColumn("date", getdate(labeledData("tfr_dt_tm")))
   
    labeledData = labeledData.withColumn("day_week", get_day_week(labeledData("tfr_dt_tm")))
           
    val get_hour = udf[Int, String]{xstr => xstr.substring(4,6).toInt }
    labeledData = labeledData.withColumn("hour", get_hour(labeledData("tfr_dt_tm")))
    
     
    //获取交易金额 （元）
    println("RMB")
    val getRMB = udf[Long, String]{xstr => (xstr.toDouble/100).toLong}
    labeledData = labeledData.withColumn("RMB", getRMB(labeledData("trans_at")))
    
    
    //交易金额的位数
    println("RMB_bits")
    val RMB_bits = udf[Int, Long]{xstr => 
      var a = xstr 
      var b =1L
      var count = -1
      while(b!=0L){
       count = count+1
       b = a%10
       a = a/10
     }
       count
    }
    labeledData = labeledData.withColumn("RMB_bits", RMB_bits(labeledData("RMB")))
     
    
    //交易金额是否1000的整数倍）
    println("is_bigRMB_500")
    val is_RMB_500 = udf[Double, Long]{xstr => any_to_double(xstr.toDouble%500 == 0)}
    labeledData = labeledData.withColumn("is_bigRMB_500", is_RMB_500(labeledData("RMB")))
    
    //交易金额是否1000的整数倍）
    println("is_bigRMB_1000")
    val is_RMB_1000 = udf[Double, Long]{xstr => any_to_double(xstr.toDouble%1000 == 0)}
    labeledData = labeledData.withColumn("is_bigRMB_1000", is_RMB_1000(labeledData("RMB")))
    
    //println("智策大额整额定义")
    println("is_large_integer")
    val is_large_integer = udf[Double, Long]{a =>
      val b = a.toString.size
      val c = a.toDouble/(math.pow(10, (b-1)))
      val d = math.abs(c-math.round(c))
      val e = d.toDouble/b.toDouble
      any_to_double(e<0.01 && a>1000)
    } 
    labeledData = labeledData.withColumn("is_large_integer", is_large_integer(labeledData("RMB")))
    
    //println("午夜交易")
    println("is_Night")
    val is_Night = udf[Double, String]{xstr => 
      val h = xstr.toInt
      val night_list = List(23,0,1,2,3,4,5)
      any_to_double(night_list.contains(h))
    }
    
    labeledData = labeledData.withColumn("is_Night", is_Night(labeledData("hour")))
    
    
    //println("交易是否需要密码  1需要    2不需要  还有其他的不管了，认为不需要   需要的更容易被伪卡")
    println("is_PW_need")
    val is_PW_need = udf[Double, String]{xstr => 
    var a = xstr.reverse.substring(0,1)  //最后一位
    if(a=="1")
      1.0
    else
      0.0
    }   
    labeledData = labeledData.withColumn("is_PW_need", is_PW_need(labeledData("pos_entry_md_cd")))
    
    //println("高危地区标识")
    println("is_highrisk_loc")
    val is_highrisk_loc = udf[Double, String]{xstr => any_to_double(IntelUtil.constUtil.HighRisk_Loc.contains(xstr.substring(0,2)))}    
    labeledData = labeledData.withColumn("is_highrisk_loc", is_highrisk_loc(labeledData("acpt_ins_id_cd_RG")))
    
    //println("高危商户")
    println("is_highrisk_MC")
    val is_highrisk_MCC = udf[Double, String]{xstr => any_to_double(IntelUtil.constUtil.HighRisk_MCC.contains(xstr))}    
    labeledData = labeledData.withColumn("is_highrisk_MCC", is_highrisk_MCC(labeledData("mchnt_tp")))
    

    
    //println("低危MCC标识")
    println("is_lowrisk_MCC")
    val is_lowrisk_MCC = udf[Double, String]{xstr => any_to_double(IntelUtil.constUtil.LowRisk_MCC.contains(xstr))}    
    labeledData = labeledData.withColumn("is_lowrisk_MCC", is_lowrisk_MCC(labeledData("mchnt_tp")))
    
    //println("成功交易")
    println("is_success")
    val is_success = udf[Double, String]{xstr => any_to_double(xstr=="00")}    
    labeledData = labeledData.withColumn("is_success", is_success(labeledData("resp_cd4")))
    
    //println("持卡人原因导致的失败交易")
    println("is_cardholder_fail")
    val cardholder_fail = udf[Double, String]{xstr => any_to_double(List("51","55","61","65","75").contains(xstr))}    
    labeledData = labeledData.withColumn("cardholder_fail", cardholder_fail(labeledData("resp_cd4")))
    
    //println("交易金额中8和9的个数")
    println("count_89")
    val count_89 = udf[Double, String]{xstr =>
      var cnt = 0
      xstr.foreach{x => if(x=='8' || x=='9') cnt = cnt+1 }
      cnt.toDouble
    }    
    labeledData = labeledData.withColumn("count_89", count_89(labeledData("trans_at")))
     
    //println("是否正常汇率")
    println("is_norm_rate")
    val is_norm_rate = udf[Double, String]{xstr => any_to_double(xstr=="30001000" || xstr=="61000000")}    
    labeledData = labeledData.withColumn("is_norm_rate", is_norm_rate(labeledData("fwd_settle_conv_rt")))
     
    
      
    
     
    //统计该卡当日交易地区总数。
    var cur_tot_locs_DF = labeledData.groupBy("pri_acct_no_conv","date").agg(countDistinct("acpt_ins_id_cd_RG") as "cur_tot_locs") 
    //cur_tot_locs_DF.show(50)
    cur_tot_locs_DF = cur_tot_locs_DF.select(cur_tot_locs_DF("pri_acct_no_conv").as("card"), cur_tot_locs_DF("date").as("pdate"), cur_tot_locs_DF("cur_tot_locs"))
    //cur_tot_locs_DF.show(50)
    
    labeledData = labeledData.join(cur_tot_locs_DF, (labeledData("pri_acct_no_conv")===cur_tot_locs_DF("card") &&  labeledData("date")===cur_tot_locs_DF("pdate")), "left_outer").drop("pdate").drop("card")
 
    //labeledData.show(50)
    
    //统计该卡当日交易省数。 countDistinct 不能再窗口函数里用，所以只能这样做
    val getProvince = udf[String, String]{xstr => xstr.substring(0,2)}
    var cur_tot_provs_DF = labeledData.groupBy("pri_acct_no_conv","date").agg(countDistinct(getProvince(labeledData("acpt_ins_id_cd_RG"))) as "cur_tot_provs") 
    cur_tot_provs_DF = cur_tot_provs_DF.select(cur_tot_provs_DF("pri_acct_no_conv").as("card"), cur_tot_provs_DF("date").as("pdate"), cur_tot_provs_DF("cur_tot_provs"))
    
    
    labeledData = labeledData.join(cur_tot_provs_DF, (labeledData("pri_acct_no_conv")===cur_tot_provs_DF("card") &&  labeledData("date")===cur_tot_provs_DF("pdate")), "left_outer").drop("card").drop("pdate")

    //labeledData.show(50)

      //统计该卡历史交易地区总数。
    var tot_locs_DF = labeledData.groupBy("pri_acct_no_conv").agg(countDistinct("acpt_ins_id_cd_RG") as "tot_locs") 
    tot_locs_DF = tot_locs_DF.select(tot_locs_DF("pri_acct_no_conv").as("card"), tot_locs_DF("tot_locs"))
    labeledData = labeledData.join(tot_locs_DF, labeledData("pri_acct_no_conv")===tot_locs_DF("card"), "left_outer").drop("card")
 
    //统计该卡历史交易省数。 countDistinct 不能再窗口函数里用，所以只能这样做 
    var tot_provs_DF = labeledData.groupBy("pri_acct_no_conv").agg(countDistinct(getProvince(labeledData("acpt_ins_id_cd_RG"))) as "tot_provs") 
    tot_provs_DF = tot_provs_DF.select(tot_provs_DF("pri_acct_no_conv").as("card"), tot_provs_DF("tot_provs"))
    labeledData = labeledData.join(tot_provs_DF, labeledData("pri_acct_no_conv")===tot_provs_DF("card"), "left_outer").drop("card")

    labeledData.registerTempTable("labeledData_TB")
    //统计卡最常用交易地区
    var most_frequent_locs_DF = ss.sql("SELECT pri_acct_no_conv, FIRST(acpt_ins_id_cd_RG) AS mlocs, MAX(cnt) AS mcnt_locs from (SELECT pri_acct_no_conv, acpt_ins_id_cd_RG, count(*) as cnt FROM labeledData_TB GROUP BY pri_acct_no_conv, acpt_ins_id_cd_RG)tmp GROUP BY pri_acct_no_conv")
    //统计卡最常用交易省
    var most_frequent_provs_DF = ss.sql("SELECT pri_acct_no_conv, FIRST(prov) AS mprovs, MAX(cnt) AS mcnt_provs from (SELECT pri_acct_no_conv, substring(acpt_ins_id_cd_RG,0,2) as prov, count(*) as cnt FROM labeledData_TB GROUP BY pri_acct_no_conv, substring(acpt_ins_id_cd_RG,0,2))tmp GROUP BY pri_acct_no_conv")
   
    most_frequent_locs_DF = most_frequent_locs_DF.select(most_frequent_locs_DF("pri_acct_no_conv").as("card"), most_frequent_locs_DF("mlocs"), most_frequent_locs_DF("mcnt_locs"))
    most_frequent_provs_DF = most_frequent_provs_DF.select(most_frequent_provs_DF("pri_acct_no_conv").as("card"), most_frequent_provs_DF("mprovs"), most_frequent_provs_DF("mcnt_provs"))
 
    
    labeledData = labeledData.join(most_frequent_locs_DF, labeledData("pri_acct_no_conv")===most_frequent_locs_DF("card"), "left_outer").drop("card")
    labeledData = labeledData.join(most_frequent_provs_DF, labeledData("pri_acct_no_conv")===most_frequent_provs_DF("card"), "left_outer").drop("card")
 
    //是否在最常用交易地区
    labeledData = labeledData.withColumn("is_frequent_locs", udf_bool_to_double(labeledData("acpt_ins_id_cd_RG")===labeledData("mlocs")))
    //是否在最常用交易省
    labeledData = labeledData.withColumn("is_frequent_provs", udf_bool_to_double(getProvince(labeledData("acpt_ins_id_cd_RG"))===labeledData("mprovs")))
    
    //该卡常用交易地区是否在高危列表
    labeledData = labeledData.withColumn("is_freq_loc_highrisk", is_highrisk_loc(labeledData("mlocs")))
    labeledData = labeledData.drop("mlocs").drop("mprovs")//.drop("mcnt_locs").drop("mcnt_provs")
    
    println("a")
    //labeledData.show(50)
    println("*******************************delta*********************************")
    val wt = Window.partitionBy("pri_acct_no_conv").orderBy("tfr_dt_tm")
    
    //labeledData = labeledData.withColumn("row_trans", functions.row_number().over(wt))
    
    
    //统计到上1笔
    val rowW_1t = wt.rowsBetween(-1, 0)  
    
    //与上比时间间隔差（分钟）
    //import org.apache.spark.sql.catalyst.expressions.DateDiff
    val format = new SimpleDateFormat("MMddHHmmss")
    val formatTime = udf[Double, String]{xstr => 
      if(xstr!=null)
        format.parse(xstr).getTime().toDouble
      else
        -1.0
    }   //注意可能会有上一次时间为Null的，就转换不了成时间了
    
    val getInterval = udf[Double, Double]{xstr => math.ceil(xstr.toFloat)}
    
    val quant_Interval = udf[Double, Double]{xstr => 
       xstr match{  
         case i if(i<=1) => 0  
         case i if(1<i && i<=2) => 1.0
         case i if(2<i && i<=5) => 2.0
         case i if(5<i && i<=10) => 3.0
         case i if(10<i && i<=60) => 4.0
         case _ => 5.0
       }
    }
    
    //上比交易时间间隔
    //labeledData = labeledData.withColumn("last_time", functions.lag("tfr_dt_tm", 1).over(wt))
 
    labeledData = labeledData.withColumn("interval_minutes_1", getInterval((formatTime(labeledData("tfr_dt_tm")) - formatTime(functions.lag("tfr_dt_tm", 1).over(wt)))/(1000*60)) )
    labeledData = labeledData.withColumn("quant_interval_1", quant_Interval((formatTime(labeledData("tfr_dt_tm")) - formatTime(functions.lag("tfr_dt_tm", 1).over(wt)))/(1000*60)) )
 
    
    //上比交易金额
    labeledData = labeledData.withColumn("last_mone_1", functions.lag("trans_at", 1).over(wt))
    
    //与上比交易金额差的绝对值
    val getAbs = udf[Double, Long]{xstr => math.abs(xstr).toDouble}
    labeledData = labeledData.withColumn("interval_money_1", getAbs(labeledData("trans_at") - labeledData("last_mone_1")))
    
    //与上比交易金额差相等
    labeledData = labeledData.withColumn("money_eq_last", udf_bool_to_double(labeledData("trans_at")===labeledData("last_mone_1")))
    
    //与上比交易金额接近
    val Long_2_Double =  udf[Double, Long]{xstr => xstr.toDouble}
    val money_near_last = udf[Double, Double]{xstr => any_to_double(xstr<=0.01)}
    labeledData = labeledData.withColumn("money_near_last",money_near_last(Long_2_Double(labeledData("interval_money_1"))/Long_2_Double(labeledData("trans_at"))))
     
    //统计该笔交易与该卡上比交易是否异地 
    labeledData = labeledData.withColumn("is_loc_changed", udf_bool_to_double(labeledData("acpt_ins_id_cd_RG").!==(functions.lag("acpt_ins_id_cd_RG", 1).over(wt)))) 
 
    println("b")
    //labeledData.show(50)
    //rows表示 行，就是前n行，后n行
    //range表示的是 具体的值，比这个值小n的行，比这个值大n的行   要用rangeBetween，orderBy( )里面的列必须是数值型的
    //println("统计当天交易")
    println("******************************cur stat************************************")
    val wd = Window.partitionBy("pri_acct_no_conv").orderBy("date")
    
    val W_cur = wd.rangeBetween(0, 0)    //当日
    labeledData = labeledData.withColumn("cur_tot_amt", sum("trans_at").over(W_cur)) //当日交易总金额
    labeledData = labeledData.withColumn("cur_tot_cnt", count("trans_at").over(W_cur)) //当日交易总次数
    labeledData = labeledData.withColumn("cur_max_amt", max("trans_at").over(W_cur)) //当日最大交易金额
    labeledData = labeledData.withColumn("cur_min_amt", min("trans_at").over(W_cur)) //当日最小交易金额
    labeledData = labeledData.withColumn("cur_avg_amt", avg("trans_at").over(W_cur)) //当日平均交易金额
      
    //labeledData = labeledData.withColumn("cur_failure_cnt", when(labeledData("is_success").===("0"), 1).otherwise(0))  //这个when其实跟udf差不多，能用gwhen尽量不用udf，因为udf可能需要频繁的序列化
    
    //统计当天失败交易的总次数
    labeledData = labeledData.withColumn("cur_failure_cnt", sum(when(labeledData("is_success").===("0"), 1).otherwise(0)).over(W_cur))
    //统计当天成功交易的总次数
    labeledData = labeledData.withColumn("cur_success_cnt", sum(when(labeledData("is_success").===("1"), 1).otherwise(0)).over(W_cur))
    //labeledData = labeledData.withColumn("cur_success_cnt", labeledData("cur_tot_cnt")-labeledData("cur_failure_cnt"))
     
    //统计当天高危地区交易的总次数
    labeledData = labeledData.withColumn("cur_highrisk_loc_cnt", sum(when(labeledData("is_highrisk_loc").===("1"), 1).otherwise(0)).over(W_cur))
    
    //统计当天高危MCC交易的总次数
    labeledData = labeledData.withColumn("cur_highrisk_MCC_cnt", sum(when(labeledData("is_highrisk_MCC").===("1"), 1).otherwise(0)).over(W_cur))
    
    //统计该卡当日查询总次数
    labeledData = labeledData.withColumn("cur_query_cnt", sum(when(labeledData("trans_id").===("S00"), 1).otherwise(0)).over(W_cur))
    
    //统计该卡当日跨境交易总次数
    labeledData = labeledData.withColumn("cur_cross_dist_cnt", sum(when(labeledData("cross_dist_in").===("1"), 1).otherwise(0)).over(W_cur))
    
    //统计该卡当日上下笔最小间隔时间
    labeledData = labeledData.withColumn("cur_cross_dist_cnt", min("interval_minutes_1").over(W_cur))
    
    //统计该卡当日上下笔平均间隔时间  (去除第一笔的NAN， 总间隔时间/总次数-1)
    labeledData = labeledData.withColumn("cur_avg_interval", (sum(when(labeledData("interval_minutes_1") !== -1.0, labeledData("interval_minutes_1")).otherwise(0)).over(W_cur))/(labeledData("cur_tot_cnt")-1))
    
    //统计该卡当日上下笔平均间隔时间在5分钟内的次数
    labeledData = labeledData.withColumn("cur_freq_cnt", sum(when(labeledData("quant_interval_1")<3, 1).otherwise(0)).over(W_cur))
     
     println("c")
    //labeledData.show(50)
    
    //println("统计前3日（不包括当日）交易")
    println("********************************3 days stat********************************")
    val W_day3 = wd.rangeBetween(-3, -1)  //累加前3天,当日除外
 
    labeledData = labeledData.withColumn("day3_tot_amt", sum("trans_at").over(W_day3))
    labeledData = labeledData.withColumn("day3_tot_cnt", count("trans_at").over(W_day3)) //3日交易总次数
    labeledData = labeledData.withColumn("day3_max_amt", max("trans_at").over(W_day3)) //3日最大交易金额
    labeledData = labeledData.withColumn("day3_min_amt", min("trans_at").over(W_day3)) //3日最小交易金额
    labeledData = labeledData.withColumn("day3_avg_amt", avg("trans_at").over(W_day3)) //3日平均交易金额
    labeledData = labeledData.withColumn("day3_fraud_cnt", sum(when(labeledData("label").===("1"), 1).otherwise(0)).over(W_day3)) //前3日被标记伪卡欺诈的次数
    
    //前3日无交易记录标志
    labeledData = labeledData.withColumn("day3_no_trans", when(labeledData("day3_tot_cnt") === 0,"1").otherwise("0"))  
    
    //前3日该卡最多的交易地区
    //labeledData = labeledData.withColumn("day3_most_locs",  labeledData.groupBy("acpt_ins_id_cd_RG").agg(count("trans_at") as "RG_counts").orderBy("RG_counts")("acpt_ins_id_cd_RG").over(W_day3))  
    
     //统计前3日失败交易的总次数
    labeledData = labeledData.withColumn("day3_failure_cnt", sum(when(labeledData("is_success").===("0"), 1).otherwise(0)).over(W_day3))
    //统计前3日成功交易的总次数
    labeledData = labeledData.withColumn("day3_success_cnt", sum(when(labeledData("is_success").===("1"), 1).otherwise(0)).over(W_day3))
    //labeledData = labeledData.withColumn("cur_success_cnt", labeledData("cur_tot_cnt")-labeledData("cur_failure_cnt"))
     
    //统计前3日高危地区交易的总次数
    labeledData = labeledData.withColumn("day3_highrisk_loc_cnt", sum(when(labeledData("is_highrisk_loc").===("1"), 1).otherwise(0)).over(W_day3))
    
    //统计前3日高危MCC交易的总次数
    labeledData = labeledData.withColumn("day3_highrisk_MCC_cnt", sum(when(labeledData("is_highrisk_MCC").===("1"), 1).otherwise(0)).over(W_day3))
    
    //统计该卡前3日查询总次数
    labeledData = labeledData.withColumn("day3_query_cnt", sum(when(labeledData("trans_id").===("S00"), 1).otherwise(0)).over(W_day3))
    
    //统计该卡前3日跨境交易总次数
    labeledData = labeledData.withColumn("day3_cross_dist_cnt", sum(when(labeledData("cross_dist_in").===("1"), 1).otherwise(0)).over(W_day3))
    
    //统计该卡前3日上下笔最小间隔时间
    labeledData = labeledData.withColumn("day3_cross_dist_cnt", min("interval_minutes_1").over(W_day3))
    
     //统计该卡前3日上下笔平均间隔时间  (去除第一笔的NAN， 总间隔时间/总次数-1)
    labeledData = labeledData.withColumn("day3_avg_interval", (sum(when(labeledData("interval_minutes_1") !== -1.0, labeledData("interval_minutes_1")).otherwise(0)).over(W_day3))/(labeledData("cur_tot_cnt")-1))
    
    //统计该卡前3日上下笔平均间隔时间在5分钟内的次数
    labeledData = labeledData.withColumn("day3_freq_cnt", sum(when(labeledData("quant_interval_1")<3, 1).otherwise(0)).over(W_day3))
     
    //前三天该卡消费的不同term数
    //ss.sql("select pri_acct_no_conv, count(distinct term_id_select) as term_cnt from (SELECT pri_acct_no_conv, first(term_id) over(partition by pri_acct_no_conv,term_id order by date range between 3 preceding and 1 preceding) as term_id_select from labeledData_TB)tmp group by pri_acct_no_conv").show  
  
     
    ///统计该卡前3日不同的交易地点总数    
    
    val w_card_loc = Window.partitionBy("pri_acct_no_conv","acpt_ins_id_cd_RG").orderBy("date")
    
    val W_card_loc_day3 = w_card_loc.rangeBetween(-3, -1)  //累加前3天,当日除外
    
    var card_loc_day3 = labeledData.select(labeledData("pri_acct_no_conv"),first("acpt_ins_id_cd_RG").over(W_card_loc_day3)).toDF("pri_acct_no_conv","loc_day3_select")
    var loc_cnt_day3 = card_loc_day3.groupBy("pri_acct_no_conv").agg(countDistinct("loc_day3_select") as "loc_day3_cnt")
    
    loc_cnt_day3 = loc_cnt_day3.select(loc_cnt_day3("pri_acct_no_conv").as("card"), loc_cnt_day3("loc_day3_cnt"))
    labeledData = labeledData.join(loc_cnt_day3, labeledData("pri_acct_no_conv")===loc_cnt_day3("card"), "left_outer").drop("card")
    
    
      
    //println("统计前7日（不包括当日）内交易")
    println("*******************************7 days stat*********************************")
    val W_day7 = wd.rangeBetween(-7, -1)  //累加前7天,当日除外
    labeledData = labeledData.withColumn("day7_tot_amt", sum("trans_at").over(W_day7))
    labeledData = labeledData.withColumn("day7_tot_cnt", count("trans_at").over(W_day7)) //7日交易总次数
    labeledData = labeledData.withColumn("day7_max_amt", max("trans_at").over(W_day7)) //7日最大交易金额
    labeledData = labeledData.withColumn("day7_min_amt", min("trans_at").over(W_day7)) //7日最小交易金额
    labeledData = labeledData.withColumn("day7_avg_amt", avg("trans_at").over(W_day7)) //7日平均交易金额
    labeledData = labeledData.withColumn("day7_fraud_cnt", sum(when(labeledData("label").===("1"), 1).otherwise(0)).over(W_day7)) //前7日被标记伪卡欺诈的次数
    
    
       //前7日无交易记录标志
    labeledData = labeledData.withColumn("day7_no_trans", when(labeledData("day7_tot_cnt") === 0,"1").otherwise("0"))  
    
    //前7日该卡最多的交易地区
    //labeledData = labeledData.withColumn("day7_most_locs",  labeledData.groupBy("acpt_ins_id_cd_RG").agg(count("trans_at") as "RG_counts").orderBy("RG_counts")("acpt_ins_id_cd_RG").over(W_day7))  
    
     //统计前7日失败交易的总次数
    labeledData = labeledData.withColumn("day7_failure_cnt", sum(when(labeledData("is_success").===("0"), 1).otherwise(0)).over(W_day7))
    //统计前7日成功交易的总次数
    labeledData = labeledData.withColumn("day7_success_cnt", sum(when(labeledData("is_success").===("1"), 1).otherwise(0)).over(W_day7))
    //labeledData = labeledData.withColumn("cur_success_cnt", labeledData("cur_tot_cnt")-labeledData("cur_failure_cnt"))
     
    //统计前7日高危地区交易的总次数
    labeledData = labeledData.withColumn("day7_highrisk_loc_cnt", sum(when(labeledData("is_highrisk_loc").===("1"), 1).otherwise(0)).over(W_day7))
    
    //统计前7日高危MCC交易的总次数
    labeledData = labeledData.withColumn("day7_highrisk_MCC_cnt", sum(when(labeledData("is_highrisk_MCC").===("1"), 1).otherwise(0)).over(W_day7))
    
    //统计该卡前7日查询总次数
    labeledData = labeledData.withColumn("day7_query_cnt", sum(when(labeledData("trans_id").===("S00"), 1).otherwise(0)).over(W_day7))
    
    //统计该卡前7日跨境交易总次数
    labeledData = labeledData.withColumn("day7_cross_dist_cnt", sum(when(labeledData("cross_dist_in").===("1"), 1).otherwise(0)).over(W_day7))
    
    //统计该卡前7日上下笔最小间隔时间
    labeledData = labeledData.withColumn("day7_cross_dist_cnt", min("interval_minutes_1").over(W_day7))
    
    //统计该卡前7日上下笔平均间隔时间  (去除第一笔的NAN， 总间隔时间/总次数-1)
    labeledData = labeledData.withColumn("day7_avg_interval", (sum(when(labeledData("interval_minutes_1") !== -1.0, labeledData("interval_minutes_1")).otherwise(0)).over(W_day7))/(labeledData("cur_tot_cnt")-1))
    
    //统计该卡前7日上下笔平均间隔时间在5分钟内的次数
    labeledData = labeledData.withColumn("day7_freq_cnt", sum(when(labeledData("quant_interval_1")<3, 1).otherwise(0)).over(W_day7))
    
     ///统计该卡前7日不同的交易地点总数    
    val W_card_loc_day7 = w_card_loc.rangeBetween(-7, -1)  //累加前7天,当日除外
    var card_loc_day7 = labeledData.select(labeledData("pri_acct_no_conv"),first("acpt_ins_id_cd_RG").over(W_card_loc_day7)).toDF("pri_acct_no_conv","loc_day7_select")
    var loc_cnt_day7 = card_loc_day7.groupBy("pri_acct_no_conv").agg(countDistinct("loc_day7_select") as "loc_day7_cnt")
    loc_cnt_day7 = loc_cnt_day7.select(loc_cnt_day7("pri_acct_no_conv").as("card"), loc_cnt_day7("loc_day7_cnt"))
    labeledData = labeledData.join(loc_cnt_day7, labeledData("pri_acct_no_conv")===loc_cnt_day7("card"), "left_outer").drop("card")
    
    println("d")
    //labeledData.show(50)
        //println("统计前30日（不包括当日）内交易")
    println("*******************************30 days stat*********************************")
    val W_day30 = wd.rangeBetween(-30, -1)  //累加前30天,当日除外
    labeledData = labeledData.withColumn("day30_tot_amt", sum("trans_at").over(W_day30))
    labeledData = labeledData.withColumn("day30_tot_cnt", count("trans_at").over(W_day30)) //30日交易总次数
    labeledData = labeledData.withColumn("day30_max_amt", max("trans_at").over(W_day30)) //30日最大交易金额
    labeledData = labeledData.withColumn("day30_min_amt", min("trans_at").over(W_day30)) //30日最小交易金额
    labeledData = labeledData.withColumn("day30_avg_amt", avg("trans_at").over(W_day30)) //30日平均交易金额
    labeledData = labeledData.withColumn("day30_fraud_cnt", sum(when(labeledData("label").===("1"), 1).otherwise(0)).over(W_day30)) //前30日被标记伪卡欺诈的次数
    
    
       //前30日无交易记录标志
    labeledData = labeledData.withColumn("day30_no_trans", when(labeledData("day30_tot_cnt") === 0,"1").otherwise("0"))  
    
    //前30日该卡最多的交易地区
    //labeledData = labeledData.withColumn("day30_most_locs",  labeledData.groupBy("acpt_ins_id_cd_RG").agg(count("trans_at") as "RG_counts").orderBy("RG_counts")("acpt_ins_id_cd_RG").over(W_day30))  
    
     //统计前30日失败交易的总次数
    labeledData = labeledData.withColumn("day30_failure_cnt", sum(when(labeledData("is_success").===("0"), 1).otherwise(0)).over(W_day30))
    //统计前30日成功交易的总次数
    labeledData = labeledData.withColumn("day30_success_cnt", sum(when(labeledData("is_success").===("1"), 1).otherwise(0)).over(W_day30))
    //labeledData = labeledData.withColumn("cur_success_cnt", labeledData("cur_tot_cnt")-labeledData("cur_failure_cnt"))
     
    //统计前30日高危地区交易的总次数
    labeledData = labeledData.withColumn("day30_highrisk_loc_cnt", sum(when(labeledData("is_highrisk_loc").===("1"), 1).otherwise(0)).over(W_day30))
    
    //统计前30日高危MCC交易的总次数
    labeledData = labeledData.withColumn("day30_highrisk_MCC_cnt", sum(when(labeledData("is_highrisk_MCC").===("1"), 1).otherwise(0)).over(W_day30))
    
    //统计该卡前30日查询总次数
    labeledData = labeledData.withColumn("day30_query_cnt", sum(when(labeledData("trans_id").===("S00"), 1).otherwise(0)).over(W_day30))
    
    //统计该卡前30日跨境交易总次数
    labeledData = labeledData.withColumn("day30_cross_dist_cnt", sum(when(labeledData("cross_dist_in").===("1"), 1).otherwise(0)).over(W_day30))
    
    //统计该卡前30日上下笔最小间隔时间
    labeledData = labeledData.withColumn("day30_cross_dist_cnt", min("interval_minutes_1").over(W_day30))
    
    //统计该卡前30日上下笔平均间隔时间  (去除第一笔的NAN， 总间隔时间/总次数-1)
    labeledData = labeledData.withColumn("day30_avg_interval", (sum(when(labeledData("interval_minutes_1") !== -1.0, labeledData("interval_minutes_1")).otherwise(0)).over(W_day30))/(labeledData("cur_tot_cnt")-1))
    
    //统计该卡前30日上下笔平均间隔时间在5分钟内的次数
    labeledData = labeledData.withColumn("day30_freq_cnt", sum(when(labeledData("quant_interval_1")<3, 1).otherwise(0)).over(W_day30))
    
     ///统计该卡前30日不同的交易地点总数    
    val W_card_loc_day30 = w_card_loc.rangeBetween(-30, -1)  //累加前30天,当日除外
    var card_loc_day30 = labeledData.select(labeledData("pri_acct_no_conv"),first("acpt_ins_id_cd_RG").over(W_card_loc_day30)).toDF("pri_acct_no_conv","loc_day30_select")
    var loc_cnt_day30 = card_loc_day30.groupBy("pri_acct_no_conv").agg(countDistinct("loc_day30_select") as "loc_day30_cnt")
    loc_cnt_day30 = loc_cnt_day30.select(loc_cnt_day30("pri_acct_no_conv").as("card"), loc_cnt_day30("loc_day30_cnt"))
    labeledData = labeledData.join(loc_cnt_day30, labeledData("pri_acct_no_conv")===loc_cnt_day30("card"), "left_outer").drop("card")
    
    println("e")
    //labeledData.show(50)
    
        //println("统计除当日外历史所有（不包括当日）内交易")
    println("*******************************all history stat*********************************")
    val W_hist = wd.rangeBetween(Long.MinValue, -1)  //除当日外历史所有
    labeledData = labeledData.withColumn("hist_tot_amt", sum("trans_at").over(W_hist))
    labeledData = labeledData.withColumn("hist_tot_cnt", count("trans_at").over(W_hist)) //30日交易总次数
    labeledData = labeledData.withColumn("hist_max_amt", max("trans_at").over(W_hist)) //30日最大交易金额
    labeledData = labeledData.withColumn("hist_min_amt", min("trans_at").over(W_hist)) //30日最小交易金额
    labeledData = labeledData.withColumn("hist_avg_amt", avg("trans_at").over(W_hist)) //30日平均交易金额
    labeledData = labeledData.withColumn("hist_fraud_cnt", sum(when(labeledData("label").===("1"), 1).otherwise(0)).over(W_hist)) //除当日外历史所有被标记伪卡欺诈的次数
    
    
       //除当日外历史所有无交易记录标志
    labeledData = labeledData.withColumn("hist_no_trans", when(labeledData("hist_tot_cnt") === 0,"1").otherwise("0"))  
    
    //除当日外历史所有该卡最多的交易地区
    //labeledData = labeledData.withColumn("hist_most_locs",  labeledData.groupBy("acpt_ins_id_cd_RG").agg(count("trans_at") as "RG_counts").orderBy("RG_counts")("acpt_ins_id_cd_RG").over(W_hist))  
    
     //统计除当日外历史所有失败交易的总次数
    labeledData = labeledData.withColumn("hist_failure_cnt", sum(when(labeledData("is_success").===("0"), 1).otherwise(0)).over(W_hist))
    //统计除当日外历史所有成功交易的总次数
    labeledData = labeledData.withColumn("hist_success_cnt", sum(when(labeledData("is_success").===("1"), 1).otherwise(0)).over(W_hist))
    //labeledData = labeledData.withColumn("cur_success_cnt", labeledData("cur_tot_cnt")-labeledData("cur_failure_cnt"))
     
    //统计除当日外历史所有高危地区交易的总次数
    labeledData = labeledData.withColumn("hist_highrisk_loc_cnt", sum(when(labeledData("is_highrisk_loc").===("1"), 1).otherwise(0)).over(W_hist))
    
    //统计除当日外历史所有高危MCC交易的总次数
    labeledData = labeledData.withColumn("hist_highrisk_MCC_cnt", sum(when(labeledData("is_highrisk_MCC").===("1"), 1).otherwise(0)).over(W_hist))
    
    //统计该卡除当日外历史所有查询总次数
    labeledData = labeledData.withColumn("hist_query_cnt", sum(when(labeledData("trans_id").===("S00"), 1).otherwise(0)).over(W_hist))
    
    //统计该卡除当日外历史所有跨境交易总次数
    labeledData = labeledData.withColumn("hist_cross_dist_cnt", sum(when(labeledData("cross_dist_in").===("1"), 1).otherwise(0)).over(W_hist))
    
    //统计该卡除当日外历史所有上下笔最小间隔时间
    labeledData = labeledData.withColumn("hist_cross_dist_cnt", min("interval_minutes_1").over(W_hist))
    
    //统计该卡除当日外历史所有上下笔平均间隔时间  (去除第一笔的NAN， 总间隔时间/总次数-1)
    labeledData = labeledData.withColumn("hist_avg_interval", (sum(when(labeledData("interval_minutes_1") !== -1.0, labeledData("interval_minutes_1")).otherwise(0)).over(W_hist))/(labeledData("cur_tot_cnt")-1))
    
    //统计该卡除当日外历史所有上下笔平均间隔时间在5分钟内的次数
    labeledData = labeledData.withColumn("hist_freq_cnt", sum(when(labeledData("quant_interval_1")<3, 1).otherwise(0)).over(W_hist))

    
    
    println("f")
    //labeledData.show(50)
    
    //短时高频
    println("*******************************frequent in short time*********************************")
    //val format = new SimpleDateFormat("MMddHHmmss")
    val timestamp_in_min = udf[Double, String]{xstr => 
      var st_time = format.parse("0701000000").getTime().toDouble
      var cur_time = format.parse(xstr).getTime().toDouble
      (cur_time - st_time)/(1000*60)
    }   //注意可能会有上一次时间为Null的，就转换不了成时间了
    
    labeledData = labeledData.withColumn("timestamp_in_min",  timestamp_in_min(labeledData("tfr_dt_tm")))

    val wt_min = Window.partitionBy("pri_acct_no_conv").orderBy("timestamp_in_min")
      
    //统计5分钟内
    println("stat in 5 mins")
    val W_min5 = wt_min.rangeBetween(-4,0)
     
    labeledData = labeledData.withColumn("min5_tot_amt", sum("trans_at").over(W_min5))
    labeledData = labeledData.withColumn("min5_tot_cnt", count("trans_at").over(W_min5)) //5 min交易总次数
    labeledData = labeledData.withColumn("min5_max_amt", max("trans_at").over(W_min5)) //5 min最大交易金额
    labeledData = labeledData.withColumn("min5_min_amt", min("trans_at").over(W_min5)) //5 min最小交易金额
    labeledData = labeledData.withColumn("min5_avg_amt", avg("trans_at").over(W_min5)) //5 min平均交易金额
    
     //前5分钟无交易记录标志
    labeledData = labeledData.withColumn("min5_no_trans", when(labeledData("min5_tot_cnt") === 1,"1").otherwise("0"))  
     
     //统计前5分钟失败交易的总次数
    labeledData = labeledData.withColumn("min5_failure_cnt", sum(when(labeledData("is_success").===("0"), 1).otherwise(0)).over(W_min5))
    //统计前5分钟成功交易的总次数
    labeledData = labeledData.withColumn("min5_success_cnt", sum(when(labeledData("is_success").===("1"), 1).otherwise(0)).over(W_min5))
   
    //统计前5分钟高危地区交易的总次数
    labeledData = labeledData.withColumn("min5_highrisk_loc_cnt", sum(when(labeledData("is_highrisk_loc").===("1"), 1).otherwise(0)).over(W_min5))
    
    //统计前5分钟高危MCC交易的总次数
    labeledData = labeledData.withColumn("min5_highrisk_MCC_cnt", sum(when(labeledData("is_highrisk_MCC").===("1"), 1).otherwise(0)).over(W_min5))
    
    //统计该卡前5分钟查询总次数
    labeledData = labeledData.withColumn("min5_query_cnt", sum(when(labeledData("trans_id").===("S00"), 1).otherwise(0)).over(W_min5))
    
    //统计该卡前5分钟跨境交易总次数
    labeledData = labeledData.withColumn("min5_cross_dist_cnt", sum(when(labeledData("cross_dist_in").===("1"), 1).otherwise(0)).over(W_min5))
 
    
   //统计15分钟内
    println("stat in 15 mins")
    val W_min15 = wt_min.rangeBetween(-14,0)
     
    labeledData = labeledData.withColumn("min15_tot_amt", sum("trans_at").over(W_min15))
    labeledData = labeledData.withColumn("min15_tot_cnt", count("trans_at").over(W_min15)) //15 min交易总次数
    labeledData = labeledData.withColumn("min15_max_amt", max("trans_at").over(W_min15)) //15 min最大交易金额
    labeledData = labeledData.withColumn("min15_min_amt", min("trans_at").over(W_min15)) //15 min最小交易金额
    labeledData = labeledData.withColumn("min15_avg_amt", avg("trans_at").over(W_min15)) //15 min平均交易金额
    
     //前15分钟无交易记录标志
    labeledData = labeledData.withColumn("min15_no_trans", when(labeledData("min15_tot_cnt") === 1,"1").otherwise("0"))  
     
     //统计前15分钟失败交易的总次数
    labeledData = labeledData.withColumn("min15_failure_cnt", sum(when(labeledData("is_success").===("0"), 1).otherwise(0)).over(W_min15))
    //统计前15分钟成功交易的总次数
    labeledData = labeledData.withColumn("min15_success_cnt", sum(when(labeledData("is_success").===("1"), 1).otherwise(0)).over(W_min15))
   
    //统计前15分钟高危地区交易的总次数
    labeledData = labeledData.withColumn("min15_highrisk_loc_cnt", sum(when(labeledData("is_highrisk_loc").===("1"), 1).otherwise(0)).over(W_min15))
    
    //统计前15分钟高危MCC交易的总次数
    labeledData = labeledData.withColumn("min15_highrisk_MCC_cnt", sum(when(labeledData("is_highrisk_MCC").===("1"), 1).otherwise(0)).over(W_min15))
    
    //统计该卡前15分钟查询总次数
    labeledData = labeledData.withColumn("min15_query_cnt", sum(when(labeledData("trans_id").===("S00"), 1).otherwise(0)).over(W_min15))
    
    //统计该卡前15分钟跨境交易总次数
    labeledData = labeledData.withColumn("min15_cross_dist_cnt", sum(when(labeledData("cross_dist_in").===("1"), 1).otherwise(0)).over(W_min15))
    
    
    
        
    
    val timestamp_in_hour = udf[Double, String]{xstr => 
      var st_time = format.parse("0701000000").getTime().toDouble
      var cur_time = format.parse(xstr).getTime().toDouble
      (cur_time - st_time)/(1000*60*60)
    }   //注意可能会有上一次时间为Null的，就转换不了成时间了
    
    labeledData = labeledData.withColumn("timestamp_in_hour",  timestamp_in_min(labeledData("tfr_dt_tm")))
    val wt_hour = Window.partitionBy("pri_acct_no_conv").orderBy("timestamp_in_hour")
    
    
    //统计1小时内
    println("stat in 1 hours")
    val W_hour1 = wt_min.rangeBetween(-1,0)
     
    labeledData = labeledData.withColumn("1hour_tot_amt", sum("trans_at").over(W_hour1))
    labeledData = labeledData.withColumn("1hour_tot_cnt", count("trans_at").over(W_hour1)) // 1 hour交易总次数
    labeledData = labeledData.withColumn("1hour_max_amt", max("trans_at").over(W_hour1)) // 1 hour最大交易金额
    labeledData = labeledData.withColumn("1hour_min_amt", min("trans_at").over(W_hour1)) // 1 hour最小交易金额
    labeledData = labeledData.withColumn("1hour_avg_amt", avg("trans_at").over(W_hour1)) // 1 hour平均交易金额
    
     //前1 小时无交易记录标志
    labeledData = labeledData.withColumn("1hour_no_trans", when(labeledData("1hour_tot_cnt") === 1,"1").otherwise("0"))  
     
     //统计前1 小时失败交易的总次数
    labeledData = labeledData.withColumn("1hour_failure_cnt", sum(when(labeledData("is_success").===("0"), 1).otherwise(0)).over(W_hour1))
    //统计前1 小时成功交易的总次数
    labeledData = labeledData.withColumn("1hour_success_cnt", sum(when(labeledData("is_success").===("1"), 1).otherwise(0)).over(W_hour1))
   
    //统计前1 小时高危地区交易的总次数
    labeledData = labeledData.withColumn("1hour_highrisk_loc_cnt", sum(when(labeledData("is_highrisk_loc").===("1"), 1).otherwise(0)).over(W_hour1))
    
    //统计前1 小时高危MCC交易的总次数
    labeledData = labeledData.withColumn("1hour_highrisk_MCC_cnt", sum(when(labeledData("is_highrisk_MCC").===("1"), 1).otherwise(0)).over(W_hour1))
    
    //统计该卡前1 小时查询总次数
    labeledData = labeledData.withColumn("1hour_query_cnt", sum(when(labeledData("trans_id").===("S00"), 1).otherwise(0)).over(W_hour1))
    
    //统计该卡前1 小时跨境交易总次数
    labeledData = labeledData.withColumn("1hour_cross_dist_cnt", sum(when(labeledData("cross_dist_in").===("1"), 1).otherwise(0)).over(W_hour1))
    
    
    
   //统计2小时内
    println("stat in 2 hours")
    val W_hour2 = wt_min.rangeBetween(-2,0)
     
    labeledData = labeledData.withColumn("2hour_tot_amt", sum("trans_at").over(W_hour2))
    labeledData = labeledData.withColumn("2hour_tot_cnt", count("trans_at").over(W_hour2)) // 1 hour交易总次数
    labeledData = labeledData.withColumn("2hour_max_amt", max("trans_at").over(W_hour2)) // 1 hour最大交易金额
    labeledData = labeledData.withColumn("2hour_min_amt", min("trans_at").over(W_hour2)) // 1 hour最小交易金额
    labeledData = labeledData.withColumn("2hour_avg_amt", avg("trans_at").over(W_hour2)) // 1 hour平均交易金额
    
     //前2小时无交易记录标志
    labeledData = labeledData.withColumn("2hour_no_trans", when(labeledData("2hour_tot_cnt") === 1,"1").otherwise("0"))  
     
     //统计前2小时失败交易的总次数
    labeledData = labeledData.withColumn("2hour_failure_cnt", sum(when(labeledData("is_success").===("0"), 1).otherwise(0)).over(W_hour2))
    //统计前2小时成功交易的总次数
    labeledData = labeledData.withColumn("2hour_success_cnt", sum(when(labeledData("is_success").===("1"), 1).otherwise(0)).over(W_hour2))
   
    //统计前2小时高危地区交易的总次数
    labeledData = labeledData.withColumn("2hour_highrisk_loc_cnt", sum(when(labeledData("is_highrisk_loc").===("1"), 1).otherwise(0)).over(W_hour2))
    
    //统计前2小时高危MCC交易的总次数
    labeledData = labeledData.withColumn("2hour_highrisk_MCC_cnt", sum(when(labeledData("is_highrisk_MCC").===("1"), 1).otherwise(0)).over(W_hour2))
    
    //统计该卡前2小时查询总次数
    labeledData = labeledData.withColumn("2hour_query_cnt", sum(when(labeledData("trans_id").===("S00"), 1).otherwise(0)).over(W_hour2))
    
    //统计该卡前2小时跨境交易总次数
    labeledData = labeledData.withColumn("2hour_cross_dist_cnt", sum(when(labeledData("cross_dist_in").===("1"), 1).otherwise(0)).over(W_hour2))
    
    
    println("g")
    //labeledData.show(50)
    
/////////////////// 等新提取好数据要加上去///////////////////
    
    /////////////////////Join FraudStatTable//////////////////////////
    //党比交易的POS机前期发生欺诈的次数统计
    var fraud_stat_term = get_FraudStat_term(ss)
    println("fraud_stat_term")
    fraud_stat_term = fraud_stat_term.withColumn("pdate", getdate(fraud_stat_term("tfr_dt_tm"))).drop("tfr_dt_tm")
    
    //fraud_stat_term.show(50)
    labeledData = labeledData.join(fraud_stat_term, (labeledData("term_id")===fraud_stat_term("term_id_fraud") &&  labeledData("date")===fraud_stat_term("pdate")), "left_outer").drop(fraud_stat_term("term_id_fraud")).drop(fraud_stat_term("pdate"))

    //党比交易的商户前期发生欺诈的次数统计
    var fraud_stat_mchnt = get_FraudStat_mchnt(ss)
    println("fraud_stat_mchnt")
    fraud_stat_mchnt = fraud_stat_mchnt.withColumn("pdate", getdate(fraud_stat_mchnt("tfr_dt_tm"))).drop("tfr_dt_tm")
    //fraud_stat_mchnt.show(50)
    labeledData = labeledData.join(fraud_stat_mchnt, (labeledData("mchnt_cd")===fraud_stat_mchnt("mchnt_cd_fraud") &&  labeledData("date")===fraud_stat_mchnt("pdate")), "left_outer").drop(fraud_stat_mchnt("mchnt_cd_fraud")).drop(fraud_stat_mchnt("pdate"))

 
    //println("高危MCC标识") 
    println("is_highrisk_MCC")
    val is_highrisk_MC = udf[Double, String]{xstr => any_to_double(IntelUtil.constUtil.Risk_mchnt_cd_List.contains(xstr))}    
    labeledData = labeledData.withColumn("is_highrisk_MC", is_highrisk_MC(labeledData("mchnt_cd")))
    
    //统计该笔交易与该卡上比交易是否同一商户 
      labeledData = labeledData.withColumn("is_MC_changed",labeledData("mchnt_cd").===(functions.lag("mchnt_cd", 1).over(wt))) 
    //println("交易金额与清算金额是否相等")
    labeledData = labeledData.withColumn("is_spec_airc", udf_bool_to_double(labeledData("trans_at")===labeledData("rcv_settle_at")))
    
    //println("无授权应答码")
    println("no_auth_id_resp_cd")
    val no_auth_id_resp_cd = udf[Double, String]{xstr => any_to_double(xstr=="N")}    
    labeledData = labeledData.withColumn("no_auth_id_resp_cd", no_auth_id_resp_cd(labeledData("auth_id_resp_cd")))
     
    //println("特殊授权应答码")
    println("is_spec_airc")
    val is_spec_airc = udf[Double, String]{xstr => any_to_double(xstr=="Y012345")}    
    labeledData = labeledData.withColumn("is_spec_airc", is_spec_airc(labeledData("auth_id_resp_cd")))
     
    //println("h")
    //labeledData.show(1000)
    //println(labeledData.columns.mkString(",")) 
      
    labeledData
  }
 
 //sys_tra_no,tfr_dt_tm,pri_acct_no_conv,term_id,trans_region,mchnt_cd,fraud_tp,date,day3_fcnt_term,day7_fcnt_term,day30_fcnt_term,day3_fcnt_mchnt,day7_fcnt_mchnt,day30_fcnt_mchnt
      def get_FraudStat_term(ss: SparkSession, filename:String = "xrli/IntelDNN/Weika/weika_term_mchnt_stat"):DataFrame = {
    		val sc = ss.sparkContext
    	  val filename = "xrli/IntelDNN/Weika/weika_term_mchnt_stat"
		    val fraud_stat_Rdd = sc.textFile(filename).map(str=> str.split(",")).map(tmparr=>Seq(tmparr(3),tmparr(1),tmparr(8),tmparr(9),tmparr(10))).map{ tmpseq=>Row.fromSeq(tmpseq)}
			    
    		val schema = StructType(StructField("term_id_fraud",StringType,true)::StructField("tfr_dt_tm",StringType,true)::StructField("day3_fcnt_term",StringType,true)::StructField("day7_fcnt_term",StringType,true)::StructField("day30_fcnt_term",StringType,true)::Nil)
		    var fraud_stat_DF = ss.createDataFrame(fraud_stat_Rdd, schema) 
			  fraud_stat_DF
     }
      
      
       def get_FraudStat_mchnt(ss: SparkSession, filename:String = "xrli/IntelDNN/Weika/weika_term_mchnt_stat"):DataFrame = {
    		val sc = ss.sparkContext
    	  val filename = "xrli/IntelDNN/Weika/weika_term_mchnt_stat"
		    val fraud_stat_Rdd = sc.textFile(filename).map(str=> str.split(",")).map(tmparr=>Seq(tmparr(5),tmparr(1),tmparr(11),tmparr(12),tmparr(13))).map{ tmpseq=>Row.fromSeq(tmpseq)}
 
    		val schema = StructType(StructField("mchnt_cd_fraud",StringType,true)::StructField("tfr_dt_tm",StringType,true)::StructField("day3_fcnt_mchnt",StringType,true)::StructField("day7_fcnt_mchnt",StringType,true)::StructField("day30_fcnt_mchnt",StringType,true)::Nil)
		    var fraud_stat_DF = ss.createDataFrame(fraud_stat_Rdd, schema) 
			  fraud_stat_DF
     }
       
       
		    
  
    
}