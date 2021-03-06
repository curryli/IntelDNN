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
import scala.reflect.runtime.universe
 

object CrossTab_NoJoin {
    val startdate = IntelUtil.varUtil.startdate
    val enddate = IntelUtil.varUtil.enddate
    val usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
    val fraudType = "62"
        
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
     
    var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate)
    
    
    var fraudType_dir = rangedir + "fraudType_filled"
    var fraudType_filled = IntelUtil.get_from_HDFS.get_processed_DF(ss, fraudType_dir)
    
    
    
    var normaldata_filled = AllData.selectExpr(usedArr_filled:_*)                                           
    
    println("normaldata_filled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
           
    val udf_Map0 = udf[Double, String]{xstr => 0.0}
    val udf_Map1 = udf[Double, String]{xstr => 1.0}
         
    var NormalData_labeled = normaldata_filled.withColumn("label", udf_Map0(normaldata_filled("trans_md_filled")))
    var fraudType_labeled = fraudType_filled.withColumn("label", udf_Map1(fraudType_filled("trans_md_filled")))
    var labeledData = fraudType_labeled.unionAll(NormalData_labeled)
     
    labeledData.describe().show
        
    val Arr_dist = labeledData.columns.toList.drop(4).dropRight(1).toArray   ///.dropRight(1)
  
 
    for(col <- Arr_dist){
      labeledData.stat.crosstab(col, "label").show
      //println(labeledData.stat.corr(col, "label"))
    }
    
    //labeledData.stat.freqItems(Arr_dist, 0.5).show()
    //println(labeledData.stat.approxQuantile("trans_at", Array(0.2,0.4,0.6,0.8), 0.2))
    
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  

    
}