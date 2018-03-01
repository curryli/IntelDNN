package API
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

object metroPay {
    val startdate = IntelUtil.varUtil.startdate
    val enddate = IntelUtil.varUtil.enddate
   
  def any_to_double[T: ClassTag](b: T):Double={
    if(b==true)
      1.0
    else
      0
  }
    
  val getdate = udf[Long, String]{xstr => xstr.substring(0,4).toLong}
    
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
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  
    import ss.implicits._
    import ss.sql
    
    val sc = ss.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
    val startTime = System.currentTimeMillis(); 
 
    val rangedir = IntelUtil.varUtil.rangeDir 
  
    var filledData = IntelUtil.get_from_HDFS.get_filled_DF(ss, "20171101", "20180131")//.persist(StorageLevel.MEMORY_AND_DISK_SER)  
    //filledData.columns.foreach {println}
    //println("filledData count:" + filledData.count())
    
    val metro_mchnts = Array("102330141110001","301330141110002","301330141110003","833331041110001","898330141110016","898330141110022","83334754111ZA01","301330141110010",
        "301330141110009","301330141110012","301330141110005","898330141110019","301330141110006","301330141110004","301330141110007","301330141110008","898330141110030")
    
    var metroData = filledData.filter(filledData("mchnt_cd_filled").isin(metro_mchnts:_*))//.persist(StorageLevel.MEMORY_AND_DISK_SER) 
    //println("metroData count:" + metroData.count()) 

    
    var metro_cards = metroData.select("pri_acct_no_conv").distinct()//.map(f=>f.getString(0))//.persist(StorageLevel.MEMORY_AND_DISK_SER) 
    //metro_cards.columns.foreach {println}
    //println("metro_cards count:" + metro_cards.count())
    //metroData.unpersist(blocking=false)
        
    var metro_related_Data = filledData.join(metro_cards, Seq("pri_acct_no_conv"), "leftsemi")//.persist(StorageLevel.MEMORY_AND_DISK_SER) 
    //metro_cards.unpersist(blocking=false)
    //println("metro_related_Data count:" + metro_related_Data.count())
    
     
    metro_related_Data.selectExpr("pri_acct_no_conv","trans_at", "tfr_dt_tm", "ext_extend_inf_28","mchnt_tp").rdd.map(_.mkString(",")).saveAsTextFile("xrli/CardholderTag/metro_related_Data")
     
      
     //获取交易金额 （元）
    println("RMB")
    val getRMB = udf[Long, String]{xstr => (xstr.toDouble/100).toLong}
    metro_related_Data = metro_related_Data.withColumn("RMB", getRMB(metro_related_Data("trans_at")))
       
    metro_related_Data = metro_related_Data.withColumn("date", getdate(metro_related_Data("tfr_dt_tm")))
    
    //sqlContext.sql("SELECT pri_acct_no_conv, count(*) FROM metro_related_Data group by pri_acct_no_conv where substring(extend_inf,28,1)='Y' or substring(extend_inf,28,1)='Z'") 
     
    println("is_QR")
    
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
      
       
    
    metro_related_Data = metro_related_Data.withColumn("is_QR", is_QR(metro_related_Data("ext_extend_inf_28")))
    
    var QR_cnt_DF = metro_related_Data.groupBy("pri_acct_no_conv").agg(sum(when(metro_related_Data("is_QR").===(1.0), 1).otherwise(0)) as "QR_cnt").persist(StorageLevel.MEMORY_AND_DISK_SER) 
    //QR_cnt_DF.filter(QR_cnt_DF("QR_cnt")>0).show
    
    val QR_cnt_ratio_DF = metro_related_Data.groupBy("pri_acct_no_conv").agg(sum(when(metro_related_Data("is_QR").===(1.0), 1).otherwise(0))/count("is_QR") as "QR_cnt_ratio")
    //QR_cnt_ratio_DF.filter(QR_cnt_ratio_DF("QR_cnt_ratio")>0).show
      
    val QR_avg_RMB_DF = metro_related_Data.filter(metro_related_Data("is_QR").===(1.0)).groupBy("pri_acct_no_conv").agg(avg("RMB") as "QR_avg_RMB")
    //QR_avg_RMB_DF.show
    
    val QR_date_cnt_DF = metro_related_Data.filter(metro_related_Data("is_QR").===(1.0)).groupBy("pri_acct_no_conv").agg(countDistinct("date") as "QR_date_cnt")
    //QR_date_cnt_DF.filter(QR_date_cnt_DF("QR_date_cnt")>1).show
    
    val QR_mcc_tps_DF = metro_related_Data.filter(metro_related_Data("is_QR").===(1.0)).groupBy("pri_acct_no_conv").agg(countDistinct("mchnt_tp") as "QR_mcc_tps")
    //QR_mcc_tps_DF.filter(QR_mcc_tps_DF("QR_mcc_tps")>1).show
    
    
    //http://blog.csdn.net/hjw199089/article/details/53535652
    //或者重命名一下
    
    QR_cnt_DF = QR_cnt_DF.join(QR_cnt_ratio_DF, Seq("pri_acct_no_conv"), "left_outer")
    QR_cnt_DF = QR_cnt_DF.join(QR_avg_RMB_DF, Seq("pri_acct_no_conv"), "left_outer")
    QR_cnt_DF = QR_cnt_DF.join(QR_date_cnt_DF, Seq("pri_acct_no_conv"), "left_outer")
    QR_cnt_DF = QR_cnt_DF.join(QR_mcc_tps_DF, Seq("pri_acct_no_conv"), "left_outer")
   
    println("calculate done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
    
    
    QR_cnt_DF = QR_cnt_DF.filter(QR_cnt_DF("QR_cnt")>0) 
    
    QR_cnt_DF.columns.foreach {println}
    
    QR_cnt_DF.rdd.map(_.mkString(",")).saveAsTextFile("xrli/CardholderTag/ValueAPI_Metro")
    
    
    
//    QR_cnt_DF.filter(QR_cnt_DF("QR_cnt")>=8 && QR_cnt_DF("QR_cnt_ratio")>=0.5 && QR_cnt_DF("QR_avg_RMB")>=100
//        && QR_cnt_DF("QR_date_cnt")>=20 && QR_cnt_DF("QR_mcc_tps")>=3)
    
    
    
    
    
    QR_cnt_DF.unpersist(blocking=false)
  
    
      println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  
//pri_acct_no_conv,QR_cnt,QR_cnt_ratio,QR_avg_RMB,QR_date_cnt,QR_mcc_tps
    
}