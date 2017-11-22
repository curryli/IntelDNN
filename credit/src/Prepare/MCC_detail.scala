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


object MCC_detail {
 
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
     
    println("is_risk_mcc")
     
    
    val caipiao_list = Array("7995","4835")
    val caipiao_mcc = udf[Double, String]{xstr => any_to_double(caipiao_list.contains(xstr))}    
    labeledData = labeledData.withColumn("is_caipiao_mcc",caipiao_mcc(labeledData("mcc_cd")))
    var count_mcc_df = labeledData.groupBy("certid").agg(sum("is_caipiao_mcc") as "caipiao_mcc_cnt") 
    
    val fakuan_list = Array("9222")
    val fakuan_mcc = udf[Double, String]{xstr => any_to_double(fakuan_list.contains(xstr))}  
    labeledData = labeledData.withColumn("is_fakuan_mcc", fakuan_mcc(labeledData("mcc_cd"))) 
    var tmpdf = labeledData.groupBy("certid").agg(sum("is_fakuan_mcc") as "fakuan_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("fakuan_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
     
    val baoshi_list = Array("9223")
    val baoshi_mcc = udf[Double, String]{xstr => any_to_double(baoshi_list.contains(xstr))}  
    labeledData = labeledData.withColumn("is_baoshi_mcc", fakuan_mcc(labeledData("mcc_cd"))) 
    tmpdf = labeledData.groupBy("certid").agg(sum("is_baoshi_mcc") as "baoshi_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("baoshi_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2")
     
    val dangpu_list = Array("9223")
    val dangpu_mcc = udf[Double, String]{xstr => any_to_double(dangpu_list.contains(xstr))}  
    labeledData = labeledData.withColumn("is_dangpu_mcc", dangpu_mcc(labeledData("mcc_cd"))) 
    tmpdf = labeledData.groupBy("certid").agg(sum("is_dangpu_mcc") as "dangpu_mcc_cnt") 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf("dangpu_mcc_cnt"))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2") 
      
    val jr_mccs = Array("5933","6051","6211","6300","6010","6011","6012","9498","6013","6050","8888","9991","5690","6014")
    
    for(col <- jr_mccs){
    var func = udf[Double, String]{xstr => any_to_double(xstr==col)} 
    var newcol = "jr_" + col
    var new_cnt = newcol + "_cnt"
    labeledData = labeledData.withColumn(newcol, func(labeledData("mcc_cd"))) 
    tmpdf = labeledData.groupBy("certid").agg(sum(newcol) as new_cnt) 
    tmpdf = tmpdf.select(tmpdf("certid").as("certid_2"), tmpdf(new_cnt))
    count_mcc_df = count_mcc_df.join(tmpdf, count_mcc_df("certid")===tmpdf("certid_2"), "left_outer").drop("certid_2") 
  }
    
    
   var label_df = ss.read.option("header", true).format("csv").option("inferSchema","true").load("xrli/credit/train_label_encrypt.csv")
    
    label_df = label_df.select(label_df("certid").as("certid_label"), label_df("label"))
    
  count_mcc_df = count_mcc_df.join(label_df, count_mcc_df("certid")===label_df("certid_label"), "left_outer").drop("certid_label")
    
   count_mcc_df = count_mcc_df.coalesce(1)
   var savepath = "xrli/credit/MCC_detail_label.csv"
   val saveOptions = Map("header" -> "true", "path" -> savepath)
   count_mcc_df.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    
    
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