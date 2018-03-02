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

object getMetroRelated {
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

    
    
    metroData.selectExpr("pri_acct_no_conv","trans_at", "tfr_dt_tm", "ext_extend_inf_28","mchnt_tp").rdd.map(_.mkString(",")).saveAsTextFile("xrli/CardholderTag/metro_Data")
      
      //所有公交闪付交易中，每种闪付交易的类型分布
    println("metroData distribute: ")
    metroData.createOrReplaceTempView("metroData")
    sqlContext.sql("select pay_tp,count(*) from (select ext_extend_inf_28 as pay_tp from metroData)A group by pay_tp").show
    
//    
//    
//    var metro_cards = metroData.select("pri_acct_no_conv").distinct()//.map(f=>f.getString(0))//.persist(StorageLevel.MEMORY_AND_DISK_SER) 
//    //metro_cards.columns.foreach {println}
//    //println("metro_cards count:" + metro_cards.count())
//    //metroData.unpersist(blocking=false)
//        
//    var metro_related_Data = filledData.join(metro_cards, Seq("pri_acct_no_conv"), "leftsemi")//.persist(StorageLevel.MEMORY_AND_DISK_SER) 
//    //metro_cards.unpersist(blocking=false)
//    //println("metro_related_Data count:" + metro_related_Data.count())
//    
//     
//    metro_related_Data.selectExpr("pri_acct_no_conv","trans_at", "tfr_dt_tm", "ext_extend_inf_28","mchnt_tp").rdd.map(_.mkString(",")).saveAsTextFile("xrli/CardholderTag/metro_related_Data")
      
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  
//pri_acct_no_conv,QR_cnt,QR_cnt_ratio,QR_avg_RMB,QR_date_cnt,QR_mcc_tps
    
}