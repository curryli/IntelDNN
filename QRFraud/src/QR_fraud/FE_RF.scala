package QR_fraud
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.OneHotEncoder


 
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.Vectors  
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.OneHotEncoder


 
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions 
import org.apache.spark.sql.expressions._

import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date

import scala.reflect.ClassTag
import getdata.get_from_hive;
import getdata.get_from_hive


object FE_RF {
   def any_to_double[T: ClassTag](b: T):Double={
    if(b==true)
      1.0
    else
      0
  }
  
   
  val udf_bool_to_double = udf[Double, Boolean]{xstr => any_to_double(xstr)} 
  val udf_int_to_double = udf[Double, Int]{xstr => any_to_double(xstr)}  
  val get_day_week = udf[Int, String]{xstr => IntelUtil.funUtil.dayForWeek(xstr)}
  
  var index_arr = Array[String]()

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);

    //    require(args.length == 3)

    val conf = new SparkConf().setAppName("QR_fraud")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
    
           
    val startTime = System.currentTimeMillis(); 
     
    var data_division = get_from_hive(hc).cache()
    
    
    var DisperseArr = IntelUtil.varUtil.DisperseArr
    
    //val DisperseArr = Array("iss_head", "iss_ins_id_cd")
         
    data_division = data_division.na.fill("NULL",DisperseArr);   // null和empty不是一回事
    
    val udf_replaceEmpty = udf[String, String]{xstr => 
        if(xstr.isEmpty())
          "NANs"
        else
          xstr
      }
 
    
   for(oldcol <- DisperseArr){   
        val newcol = oldcol + "_filled" 
        val col_cnt = data_division.select(oldcol).distinct().count()
        if(col_cnt<500){
          println(oldcol , " count: ", col_cnt)
          index_arr = index_arr.+:(oldcol)
          data_division = data_division.withColumn(newcol, udf_replaceEmpty(data_division(oldcol)))

          var indexCat = oldcol + "_CatVec"
          var indexer = new StringIndexer().setInputCol(newcol).setOutputCol(indexCat).setHandleInvalid("skip")
          data_division = indexer.fit(data_division).transform(data_division)
        }
      }
       
    
    DisperseArr = index_arr
     
    println("data_division index done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
             
    data_division.show(10) 
    
    
    
      ///////////////////////////////////////////////////////////
      
    data_division = data_division.withColumn("day_week", get_day_week(data_division("pdate")))
           
    val get_hour = udf[Int, String]{xstr => xstr.substring(8,10).toInt }
    data_division = data_division.withColumn("hour", get_hour(data_division("trans_tm")))
    
     //println("午夜交易")
    println("is_Night")
    val is_Night = udf[Double, String]{xstr => 
      val h = xstr.toInt
      val night_list = List(23,0,1,2,3,4,5)
      any_to_double(night_list.contains(h))
    }
    
    data_division = data_division.withColumn("is_Night", is_Night(data_division("hour")))
    
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
    data_division = data_division.withColumn("RMB_bits", RMB_bits(data_division("trans_at")))
    
     //println("智策大额整额定义")
    println("is_large_integer")
    val is_large_integer = udf[Double, Long]{a =>
      val b = a.toString.size
      val c = a.toDouble/(math.pow(10, (b-1)))
      val d = math.abs(c-math.round(c))
      val e = d.toDouble/b.toDouble
      any_to_double(e<0.01 && a>1000)
    } 
    data_division = data_division.withColumn("is_large_integer", is_large_integer(data_division("trans_at")))
   
    
    //println("交易金额中8和9的个数")
    println("count_89")
    val count_89 = udf[Double, String]{xstr =>
      var cnt = 0
      xstr.foreach{x => if(x=='8' || x=='9') cnt = cnt+1 }
      cnt.toDouble
    }    
    data_division = data_division.withColumn("count_89", count_89(data_division("trans_at")))
    
    
    val count_89_ratio = udf[Double, String]{xstr =>
      var cnt = 0
      xstr.foreach{x => if(x=='8' || x=='9') cnt = cnt+1 }
      cnt.toDouble/(xstr.length().toDouble)
    }  
    data_division = data_division.withColumn("count_89_ratio", count_89_ratio(data_division("trans_at")))
    
   
    val delta_time =  (start: String, end: String, interval: String) => {
      IntelUtil.funUtil.getDeltaTime(start, end, interval)
    }
   
    val udf_delta_time = udf(delta_time)
    
    data_division = data_division.withColumn("delta_time", udf_delta_time(data_division("ls_trans_tm"), data_division("trans_tm"), lit("seconds")))
    
    
    val delta_at =  (ls_trans_at: Double, trans_at: Double) => {
       math.abs(trans_at - ls_trans_at)
    }
   
    val udf_delta_at = udf(delta_at)
    
    data_division = data_division.withColumn("delta_at", udf_delta_at(data_division("trans_at"), data_division("ls_trans_at")))
     
    
    ///////////////
   
    //统计该卡当日交易地区总数。
    var cur_tot_locs_DF = data_division.groupBy("pri_acct_no","pdate").agg(countDistinct("area_cd") as "cur_tot_locs") 
    cur_tot_locs_DF = cur_tot_locs_DF.select(cur_tot_locs_DF("pri_acct_no").as("card"), cur_tot_locs_DF("pdate").as("date"), cur_tot_locs_DF("cur_tot_locs"))
    data_division = data_division.join(cur_tot_locs_DF, (data_division("pri_acct_no")===cur_tot_locs_DF("card") &&  data_division("pdate")===cur_tot_locs_DF("date")), "left_outer").drop("date").drop("card")

    //统计该卡历史交易地区总数。
    var tot_locs_DF = data_division.groupBy("pri_acct_no").agg(countDistinct("area_cd") as "tot_locs") 
    tot_locs_DF = tot_locs_DF.select(tot_locs_DF("pri_acct_no").as("card"), tot_locs_DF("tot_locs"))
    data_division = data_division.join(tot_locs_DF, data_division("pri_acct_no")===tot_locs_DF("card"), "left_outer").drop("card")
  
   
    val wt = Window.partitionBy("pri_acct_no").orderBy("trans_tm")
     
    //统计到上1笔
    val rowW_1t = wt.rowsBetween(-1, 0)  
    
    //与上比时间间隔差（分钟）
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
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
    data_division = data_division.withColumn("interval_minutes_1", getInterval((formatTime(data_division("trans_tm")) - formatTime(functions.lag("trans_tm", 1).over(wt)))/(1000*60)) )
    data_division = data_division.withColumn("quant_interval_1", quant_Interval((formatTime(data_division("trans_tm")) - formatTime(functions.lag("trans_tm", 1).over(wt)))/(1000*60)) )
  
    //上比交易金额
    data_division = data_division.withColumn("last_money_1", functions.lag("trans_at", 1).over(wt))
    
    //与上比交易金额差的绝对值
    val getAbs = udf[Double, Long]{xstr => math.abs(xstr).toDouble}
    data_division = data_division.withColumn("interval_money_1", getAbs(data_division("trans_at") - data_division("last_money_1")))
    
    //与上比交易金额差相等
    data_division = data_division.withColumn("money_eq_last", udf_bool_to_double(data_division("trans_at")===data_division("last_money_1")))
    
    //与上比交易金额接近
    val Long_2_Double =  udf[Double, Long]{xstr => xstr.toDouble}
    val money_near_last = udf[Double, Double]{xstr => any_to_double(xstr<=0.01)}
    data_division = data_division.withColumn("money_near_last",money_near_last(Long_2_Double(data_division("interval_money_1"))/Long_2_Double(data_division("trans_at"))))
     
      
    
    
    
    println("******************************cur stat************************************")
    
    
  
   
    val udf_str_to_long = udf[Long, String]{xstr => xstr.toLong} 
    data_division = data_division.withColumn("pdate_long", udf_str_to_long(data_division("pdate")))
    
    val wd = Window.partitionBy("pri_acct_no").orderBy("pdate_long")
    
    val W_cur = wd.rangeBetween(0, 0)    //当日
    data_division = data_division.withColumn("cur_tot_amt", sum("trans_at").over(W_cur)) //当日交易总金额
    data_division = data_division.withColumn("cur_tot_cnt", count("trans_at").over(W_cur)) //当日交易总次数
    data_division = data_division.withColumn("cur_max_amt", max("trans_at").over(W_cur)) //当日最大交易金额
    data_division = data_division.withColumn("cur_min_amt", min("trans_at").over(W_cur)) //当日最小交易金额
    data_division = data_division.withColumn("cur_avg_amt", avg("trans_at").over(W_cur)) //当日平均交易金额
      
    //统计该卡当日上下笔最小间隔时间
    data_division = data_division.withColumn("min_interval_minutes_1", min("interval_minutes_1").over(W_cur))
    
    //统计该卡当日上下笔平均间隔时间  (去除第一笔的NAN， 总间隔时间/总次数-1)
    data_division = data_division.withColumn("cur_avg_interval", (sum(when(data_division("interval_minutes_1") !== -1.0, data_division("interval_minutes_1")).otherwise(0)).over(W_cur))/(data_division("cur_tot_cnt")-1))
    
    //统计该卡当日上下笔平均间隔时间在5分钟内的次数
    data_division = data_division.withColumn("cur_freq_cnt", sum(when(data_division("quant_interval_1")<3, 1).otherwise(0)).over(W_cur))
     
      
    
    
    //println("统计除当日外历史所有（不包括当日）内交易")
    println("*******************************all history stat*********************************")
    val W_hist = wd.rangeBetween(Long.MinValue, -1)  //除当日外历史所有
    data_division = data_division.withColumn("hist_tot_amt", sum("trans_at").over(W_hist))
    data_division = data_division.withColumn("hist_tot_cnt", count("trans_at").over(W_hist))  
    data_division = data_division.withColumn("hist_max_amt", max("trans_at").over(W_hist)) 
    data_division = data_division.withColumn("hist_min_amt", min("trans_at").over(W_hist))  
    data_division = data_division.withColumn("hist_avg_amt", avg("trans_at").over(W_hist))  
 
    //除当日外历史所有无交易记录标志
    data_division = data_division.withColumn("hist_no_trans", when(data_division("hist_tot_cnt") === 0,1.0).otherwise(0.0))  
    
    
    
    //短时高频
    println("*******************************frequent in short time*********************************")

    val timestamp_in_min = udf[Double, String]{xstr => 
      var st_time = format.parse("20171201000000").getTime().toDouble
      var cur_time = format.parse(xstr).getTime().toDouble
      (cur_time - st_time)/(1000*60)
    }  
    
    data_division = data_division.withColumn("timestamp_in_min",  timestamp_in_min(data_division("trans_tm")))
    
    
    //统计15分钟内
    println("stat in 15 mins")
    val wt_min = Window.partitionBy("pri_acct_no").orderBy("timestamp_in_min")
    val W_min15 = wt_min.rangeBetween(-14,0)
     
    data_division = data_division.withColumn("min15_tot_amt", sum("trans_at").over(W_min15))
    data_division = data_division.withColumn("min15_tot_cnt", count("trans_at").over(W_min15)) //15 min交易总次数
    data_division = data_division.withColumn("min15_max_amt", max("trans_at").over(W_min15)) //15 min最大交易金额
    data_division = data_division.withColumn("min15_min_amt", min("trans_at").over(W_min15)) //15 min最小交易金额
    data_division = data_division.withColumn("min15_avg_amt", avg("trans_at").over(W_min15)) //15 min平均交易金额
    
    data_division = data_division.withColumn("min15_no_trans", when(data_division("min15_tot_cnt") === 1,1.0).otherwise(0.0))   //前15分钟无交易记录标志
    
    
     //统计1小时内
    println("stat in 1 hours")
    val timestamp_in_hour = udf[Double, String]{xstr => 
      var st_time = format.parse("20171201000000").getTime().toDouble
      var cur_time = format.parse(xstr).getTime().toDouble
      (cur_time - st_time)/(1000*60*60)
    } 
    
    data_division = data_division.withColumn("timestamp_in_hour",  timestamp_in_min(data_division("trans_tm")))
    
    
    val wt_hour = Window.partitionBy("pri_acct_no").orderBy("timestamp_in_hour")
    val W_hour1 = wt_hour.rangeBetween(-1,0)
     
    data_division = data_division.withColumn("1hour_tot_amt", sum("trans_at").over(W_hour1))
    data_division = data_division.withColumn("1hour_tot_cnt", count("trans_at").over(W_hour1)) // 1 hour交易总次数
    data_division = data_division.withColumn("1hour_max_amt", max("trans_at").over(W_hour1)) // 1 hour最大交易金额
    data_division = data_division.withColumn("1hour_min_amt", min("trans_at").over(W_hour1)) // 1 hour最小交易金额
    data_division = data_division.withColumn("1hour_avg_amt", avg("trans_at").over(W_hour1)) // 1 hour平均交易金额
    
    data_division = data_division.withColumn("1hour_no_trans", when(data_division("1hour_tot_cnt") === 1,1.0).otherwise(0.0))  //前1 小时无交易记录标志
     
  
    
    println(data_division.columns.mkString(","))
    
    
    ///////////////////////////////////////////////////////
    //////////////////////////////////////////////////////
    val CatVecArr = DisperseArr.map { x => x + "_CatVec"}
    
    val used_arr = IntelUtil.varUtil.ori_sus_Arr.++( IntelUtil.varUtil.calc_cols).++(CatVecArr)
    
    data_division = data_division.selectExpr(used_arr.+:("label").+:("division"):_*).cache()
     
    
    data_division = data_division.na.fill(0, used_arr)
    data_division = data_division.na.drop()
    
    val assembler1 = new VectorAssembler()
      .setInputCols(used_arr)
      .setOutputCol("featureVector")
     
    data_division = assembler1.transform(data_division)
    println("assembler1 dataframe")
    data_division.show(10) 
      
      
    val normalizer1 = new Normalizer().setInputCol("featureVector").setOutputCol("normFeatures")     //默认是L2
    data_division = normalizer1.transform(data_division)
     
    println("normalizer dataframe")

    data_division.show(10)
     
    var normal_train = data_division.filter(data_division("division")=== "normal_train")
    var normal_test = data_division.filter(data_division("division")=== "normal_test")
    var fraud_train = data_division.filter(data_division("division")=== "fraud_train")
    var fraud_test = data_division.filter(data_division("division")=== "fraud_test")
   
    val trainingData = normal_train.sample(false, 0.005).unionAll(fraud_train).cache()
    val testData = normal_test.unionAll(fraud_test).cache()
    
    data_division.unpersist(blocking=false)
    
    println("trainingData.count: ", trainingData.count, " testData.count: ", testData.count)
     
    trainingData.selectExpr(used_arr.+:("label"):_*).rdd.map(_.mkString(",")).saveAsTextFile("xrli/QRfraud/trainingData_new")
    testData.selectExpr(used_arr.+:("label"):_*).rdd.map(_.mkString(",")).saveAsTextFile("xrli/QRfraud/testData_new")
     
    
     
    println("Save done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
    val rfClassifier = new RandomForestClassifier()
        .setLabelCol("label")
        .setFeaturesCol("featureVector")
        .setNumTrees(200)
        .setSubsamplingRate(0.7)
        .setFeatureSubsetStrategy("auto")
        .setThresholds(Array(180,1))
         
        .setImpurity("gini")
        .setMaxDepth(5)
        .setMaxBins(10000)
     
       
      
    val model = rfClassifier.fit(trainingData)
     
    println("training done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
    
  
       
    val predictionResult = model.transform(testData)
        
    val eval_result = IntelUtil.funUtil.get_CF_Matrix(predictionResult)
     
    println("Current Precision_P is: " + eval_result.Precision_P)
    println("Current Recall_P is: " + eval_result.Recall_P)
      
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setMetricName("areaUnderROC")
       
    val accuracy = evaluator.evaluate(predictionResult) //AUC
    
    println("accuracy is: " + accuracy)
    
     
    val featureImportance = model.featureImportances.toSparse
    val topFeatures = used_arr.zip(featureImportance.values).sortBy( - _._2) 
    
    topFeatures.foreach(println)



    println("FE done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
  }
   
  
}