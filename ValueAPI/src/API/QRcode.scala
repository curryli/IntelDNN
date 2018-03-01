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

object QRcode {
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
      .getOrCreate()
  
    import ss.implicits._
    import ss.sql
    
    val sc = ss.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
    val startTime = System.currentTimeMillis(); 
 
    val rangedir = IntelUtil.varUtil.rangeDir 
  
    var filledData = IntelUtil.get_from_HDFS.get_filled_DF(ss, "20171101", "20180131")
//    println("filledData count:" + filledData.count())
    
     //获取交易金额 （元）
    println("RMB")
    val getRMB = udf[Long, String]{xstr => (xstr.toDouble/100).toLong}
    filledData = filledData.withColumn("RMB", getRMB(filledData("trans_at")))
       
    filledData = filledData.withColumn("date", getdate(filledData("tfr_dt_tm")))
    
    //sqlContext.sql("SELECT pri_acct_no_conv, count(*) FROM filledData group by pri_acct_no_conv where substring(extend_inf,28,1)='Y' or substring(extend_inf,28,1)='Z'") 
     
    println("is_QR")
    val is_QR = udf[Double, String]{xstr => 
      if(xstr.length()<28)
        0.0
      else{
        var a = xstr.charAt(26)  //最后一位
        if(a=='Y' || a=='Z')  // if(a>='1' & a<='9')  //
          1.0
        else
          0.0
      }
    }  
    
    filledData = filledData.withColumn("is_QR", is_QR(filledData("extend_inf")))
    
    var QR_cnt_DF = filledData.groupBy("pri_acct_no_conv").agg(sum(when(filledData("is_QR").===(1.0), 1).otherwise(0)) as "QR_cnt").persist(StorageLevel.MEMORY_AND_DISK_SER) 
    //QR_cnt_DF.filter(QR_cnt_DF("QR_cnt")>0).show
    
    val QR_cnt_ratio_DF = filledData.groupBy("pri_acct_no_conv").agg(sum(when(filledData("is_QR").===(1.0), 1).otherwise(0))/count("is_QR") as "QR_cnt_ratio")
    //QR_cnt_ratio_DF.filter(QR_cnt_ratio_DF("QR_cnt_ratio")>0).show
      
    val QR_avg_RMB_DF = filledData.filter(filledData("is_QR").===(1.0)).groupBy("pri_acct_no_conv").agg(avg("RMB") as "QR_avg_RMB")
    //QR_avg_RMB_DF.show
    
    val QR_date_cnt_DF = filledData.filter(filledData("is_QR").===(1.0)).groupBy("pri_acct_no_conv").agg(countDistinct("date") as "QR_date_cnt")
    //QR_date_cnt_DF.filter(QR_date_cnt_DF("QR_date_cnt")>1).show
    
    val QR_mcc_tps_DF = filledData.filter(filledData("is_QR").===(1.0)).groupBy("pri_acct_no_conv").agg(countDistinct("mchnt_tp") as "QR_mcc_tps")
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
    
    QR_cnt_DF.rdd.map(_.mkString(",")).saveAsTextFile("xrli/CardholderTag/ValueAPI_QRCode")
    
    QR_cnt_DF.unpersist(blocking=false)
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  
//pri_acct_no_conv,QR_cnt,QR_cnt_ratio,QR_avg_RMB,QR_date_cnt,QR_mcc_tps
    
}