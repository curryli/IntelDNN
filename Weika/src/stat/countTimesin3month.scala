package stat
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


object countTimesin3month {
    val startdate = IntelUtil.varUtil.startdate
    val enddate = IntelUtil.varUtil.enddate
    
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
 
    val rangedir = IntelUtil.varUtil.testDir 
     


    var FE_Data = IntelUtil.get_from_HDFS.get_FE_DF(ss, rangedir + "FE_db").persist(StorageLevel.MEMORY_AND_DISK_SER)
     
     val label= "label" 
     val cardcol= "pri_acct_no_conv"
    
    
    val countdf = FE_Data.groupBy("pri_acct_no_conv").agg(count("trans_at") as "counts")
    
    val filtereddf0 = countdf.filter(countdf("counts")>0 && countdf("counts")<=5) 
    val filtereddf1 = countdf.filter(countdf("counts")>5 && countdf("counts")<=10) 
    val filtereddf2 = countdf.filter(countdf("counts")>10 && countdf("counts")<=15) 
    val filtereddf3 = countdf.filter(countdf("counts")>15 && countdf("counts")<=20) 
    val filtereddf4 = countdf.filter(countdf("counts")>20 && countdf("counts")<=25) 
    val filtereddf5 = countdf.filter(countdf("counts")>25 && countdf("counts")<=30)
    val filtereddf6 = countdf.filter(countdf("counts")>30 && countdf("counts")<=35)
    val filtereddf7 = countdf.filter(countdf("counts")>35 && countdf("counts")<=40)
    val filtereddf8 = countdf.filter(countdf("counts")>40 && countdf("counts")<=45)
    val filtereddf9 = countdf.filter(countdf("counts")>45 && countdf("counts")<=50)
    val filtereddf10 = countdf.filter(countdf("counts")>50 && countdf("counts")<=55)
    val filtereddf11 = countdf.filter(countdf("counts")>55 && countdf("counts")<=60)
    val filtereddf12 = countdf.filter(countdf("counts")>60 && countdf("counts")<=65)
    val filtereddf13 = countdf.filter(countdf("counts")>65 && countdf("counts")<=70)
    val filtereddf14 = countdf.filter(countdf("counts")>70 && countdf("counts")<=75)
    val filtereddf15 = countdf.filter(countdf("counts")>75 && countdf("counts")<=80)
    val filtereddf16 = countdf.filter(countdf("counts")>80 && countdf("counts")<=85)
    val filtereddf17 = countdf.filter(countdf("counts")>85 && countdf("counts")<=90)
    val filtereddf18 = countdf.filter(countdf("counts")>90 && countdf("counts")<=95)
    val filtereddf19 = countdf.filter(countdf("counts")>95 && countdf("counts")<=100)
    val filtereddf20 = countdf.filter(countdf("counts")>100)
    
    println("5: " + filtereddf0.count())
    println("10: " + filtereddf1.count())
    println("15: " + filtereddf2.count())
    println("20: " + filtereddf3.count())
    println("25: " + filtereddf4.count())
    println("30: " + filtereddf5.count())
    println("35: " + filtereddf6.count())
    println("40: " + filtereddf7.count())
    println("45: " + filtereddf8.count())
    println("50: " + filtereddf9.count())
    
      println("55: " + filtereddf10.count())
    println("60: " + filtereddf11.count())
    println("65: " + filtereddf12.count())
    println("70: " + filtereddf13.count())
    println("75: " + filtereddf14.count())
    println("80: " + filtereddf15.count())
    println("85: " + filtereddf16.count())
    println("90: " + filtereddf17.count())
    println("95: " + filtereddf18.count())
    println("100: " + filtereddf19.count())
    println("above: " + filtereddf20.count())
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  
 

//3个月    
 
    
}