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

object General {
  val startdate = IntelUtil.varUtil.startdate
  val enddate = IntelUtil.varUtil.enddate
  
  val Time_span =3
   
  def any_to_double[T: ClassTag](b: T):Double={
    if(b==true)
      1.0
    else
      0
  }
    
  val getdate = udf[Long, String]{xstr => xstr.substring(0,4).toLong}
  val getmonth = udf[Long, String]{xstr => xstr.substring(0,2).toLong}
    
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
  
    var filledData = IntelUtil.get_from_HDFS.get_filled_DF(ss, "20171201", "20171201")
    
     //获取交易金额 （元）
    println("RMB")
    val getRMB = udf[Long, String]{xstr => (xstr.toDouble/100).toLong}
    filledData = filledData.withColumn("RMB", getRMB(filledData("trans_at")))
       
    filledData = filledData.withColumn("date", getdate(filledData("tfr_dt_tm")))
    filledData = filledData.withColumn("month", getmonth(filledData("tfr_dt_tm")))
    
    
    
    var RMB_sum_DF = filledData.groupBy("pri_acct_no_conv").agg(sum("RMB") as "RMB_sum").persist(StorageLevel.MEMORY_AND_DISK_SER)    //有消费的月均消费
    var RMB_avg_DF = filledData.groupBy("pri_acct_no_conv").agg(avg("RMB") as "RMB_avg")  //笔均消费
    //var RMB_sum_DF = filledData.groupBy("pri_acct_no_conv").agg(variance("RMB") as "RMB_var")  //笔均消费方差
    var RMB_std_DF = filledData.groupBy("pri_acct_no_conv").agg(stddev("RMB") as "RMB_std")  //笔均消费标准差
    var tran_cnt_DF = filledData.groupBy("pri_acct_no_conv").agg(count("RMB") as "tran_cnt")  //消费总次数
        
    var has_trans_date_DF = filledData.groupBy("pri_acct_no_conv").agg(countDistinct("date") as "has_trans_date") //有消费的月份
    var has_trans_month_DF = filledData.groupBy("pri_acct_no_conv").agg(countDistinct("month") as "has_trans_month") //有消费的月份
    //var RMB_per_Month_DF = filledData.groupBy("pri_acct_no_conv").agg(sum("RMB")/countDistinct("month") as "RMB_per_Month")  //有消费的月均消费
    var RMB_per_Month_DF = filledData.groupBy("pri_acct_no_conv").agg(sum("RMB")/Time_span as "RMB_per_Month")  //月均消费金额
    var cnt_per_Month_DF = filledData.groupBy("pri_acct_no_conv").agg(count("RMB")/Time_span as "cnt_per_Month")  //月均消费次数

    RMB_sum_DF = RMB_sum_DF.join(RMB_avg_DF, Seq("pri_acct_no_conv"), "left_outer")
    RMB_sum_DF = RMB_sum_DF.join(RMB_std_DF, Seq("pri_acct_no_conv"), "left_outer")
    RMB_sum_DF = RMB_sum_DF.join(tran_cnt_DF, Seq("pri_acct_no_conv"), "left_outer")
    RMB_sum_DF = RMB_sum_DF.join(has_trans_date_DF, Seq("pri_acct_no_conv"), "left_outer")
    RMB_sum_DF = RMB_sum_DF.join(has_trans_month_DF, Seq("pri_acct_no_conv"), "left_outer")
    RMB_sum_DF = RMB_sum_DF.join(RMB_per_Month_DF, Seq("pri_acct_no_conv"), "left_outer")
    RMB_sum_DF = RMB_sum_DF.join(cnt_per_Month_DF, Seq("pri_acct_no_conv"), "left_outer") 
    
    
    println("calculate done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
    
    RMB_sum_DF.show(100)
    
    RMB_sum_DF.unpersist(blocking=false)
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  

    
}