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
 

object stable {
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
    
    val sc = ss.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
    val startTime = System.currentTimeMillis(); 
 
    val rangedir = IntelUtil.varUtil.rangeDir 
  
    var filledData = IntelUtil.get_from_HDFS.get_filled_DF(ss, "20160901", "20160901")
    filledData.show
    
     //获取交易金额 （元）
    println("RMB")
    val getRMB = udf[Long, String]{xstr => (xstr.toDouble/100).toLong}
    filledData = filledData.withColumn("RMB", getRMB(filledData("trans_at")))
    
    filledData.groupBy("pri_acct_no_conv").agg(avg("RMB") as "avg_RMB").show 
    
    filledData.createOrReplaceTempView("filledData")
    
    sqlContext.sql("SELECT pri_acct_no_conv, avg(RMB) as RMB_avg FROM filledData group by pri_acct_no_conv").show

    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  

    
}