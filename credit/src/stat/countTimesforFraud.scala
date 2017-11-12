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
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification._
 

object countTimesforFraud {
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
 
    val rangedir = IntelUtil.varUtil.rangeDir 
     
//    var input_dir = rangedir + "Labeled_All"
//    var labeledData = IntelUtil.get_from_HDFS.get_labeled_DF(ss, input_dir).persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
//    labeledData.show(10)
//    
//    //去除借记卡 
//    var fraudCards = labeledData.filter(labeledData("label").===(1)).select("pri_acct_no_conv").distinct()
//    println("fraudCards: " + fraudCards.count())
//    
//    var fraudrelatedData = labeledData.join(fraudCards, labeledData("pri_acct_no_conv")===fraudCards("pri_acct_no_conv"), "leftsemi")
//    println("fraudrelatedData: " + fraudrelatedData.count()) 
//
//    val countdf = fraudrelatedData.groupBy("pri_acct_no_conv").agg(count("trans_at") as "counts")  
     
    
    var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000) 
    val countdf = AllData.groupBy("pri_acct_no_conv").agg(count("trans_at") as "counts")
    
    val filtereddf0 = countdf.filter(countdf("counts")>0 && countdf("counts")<=5) 
    val filtereddf1 = countdf.filter(countdf("counts")>5 && countdf("counts")<=10) 
    val filtereddf2 = countdf.filter(countdf("counts")>10 && countdf("counts")<=20) 
    val filtereddf3 = countdf.filter(countdf("counts")>20 && countdf("counts")<=30) 
    val filtereddf4 = countdf.filter(countdf("counts")>30 && countdf("counts")<=40) 
    val filtereddf5 = countdf.filter(countdf("counts")>40 && countdf("counts")<=50)
    val filtereddf6 = countdf.filter(countdf("counts")>50 && countdf("counts")<=100)
    val filtereddf7 = countdf.filter(countdf("counts")>100 && countdf("counts")<=200)
    val filtereddf8 = countdf.filter(countdf("counts")>200 && countdf("counts")<=500)
    val filtereddf9 = countdf.filter(countdf("counts")>500)
    
    println("0~5: " + filtereddf0.count())
    println("5~10: " + filtereddf1.count())
    println("10~20: " + filtereddf2.count())
    println("20~30: " + filtereddf3.count())
    println("30~40: " + filtereddf4.count())
    println("40~50: " + filtereddf5.count())
    println("50~100: " + filtereddf6.count())
    println("100~200: " + filtereddf7.count())
    println("200~500: " + filtereddf8.count())
    println(">500: " + filtereddf9.count())
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  
//9月份欺诈分布：
//0~5: 438
//5~10: 472
//10~20: 558
//20~30: 181
//30~40: 55
//40~50: 25
//50~100: 17
//100~200: 1
//200~500: 1
//>500: 0

//一天整体    
//0~5: 140452054
//5~10: 31370637
//10~20: 12439847
//20~30: 1972316
//30~40: 495667
//40~50: 172803
//50~100: 164992
//100~200: 29439
    
}