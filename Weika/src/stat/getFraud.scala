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
import org.apache.spark.storage.StorageLevel
 

object getFraud {
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
     
    var input_dir = rangedir + "Labeled_All"
    
    
    
     var labeledData1 = IntelUtil.get_from_HDFS.get_labeled_DF(ss, "xrli/IntelDNN/Weika/201607/Labeled_All" )//.persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    var labeledData2 = IntelUtil.get_from_HDFS.get_labeled_DF(ss, "xrli/IntelDNN/Weika/201608/Labeled_All" )//.persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    var labeledData3 = IntelUtil.get_from_HDFS.get_labeled_DF(ss, "xrli/IntelDNN/Weika/201609/Labeled_All" )
    //var labeledData4 = IntelUtil.get_from_HDFS.get_labeled_DF(ss, "xrli/IntelDNN/CashOut/201610/Labeled_All" )
    
    var labeledData = labeledData1.unionAll(labeledData2).unionAll(labeledData3)//.unionAll(labeledData4)
    var fraudData = labeledData.filter(labeledData("label").===(1)) 
    val Arr_dist = fraudData.columns.toList.toArray
    Arr_dist.foreach { println }
    fraudData.selectExpr(Arr_dist:_*).rdd.map(_.mkString(",")).saveAsTextFile("xrli/IntelDNN/Weika/weika_allfraud_0709")
    
    labeledData1.selectExpr(Arr_dist:_*).rdd.map(_.mkString(",")).saveAsTextFile("xrli/IntelDNN/Weika/label_all_07")
    
//    var labeledData = IntelUtil.get_from_HDFS.get_labeled_DF(ss, input_dir).persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
//    labeledData.show(10)
//    
//    //去除借记卡 
//    var fraudData = labeledData.filter(labeledData("label").===(1)) 
//    
//    val card_media1 = fraudData.filter(fraudData("card_media").===(1)) 
//    val card_media2 = fraudData.filter(fraudData("card_media").===(2))
//    val card_media3 = fraudData.filter(fraudData("card_media").===(3))
//    val card_media4 = fraudData.filter(fraudData("card_media").===(4))
//    
//    println("card_media:" + "1: " + card_media1.count + "  2: " + card_media2.count + "  3: " + card_media3.count + "  4: " + card_media4.count)
//    
//    val cross_dist0 = fraudData.filter(fraudData("cross_dist_in").===(0))
//    val cross_dist1 = fraudData.filter(fraudData("cross_dist_in").===(1))
//    println("cross_dist:" + "0: " + cross_dist0.count + "  1: " + cross_dist1.count)
//
//    val Arr_dist = labeledData.columns.toList.drop(4).dropRight(1).toArray
//    
//    for(col <- Arr_dist){
//      labeledData.stat.crosstab(col, "label").show
//    }
//    
//    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  

    
}