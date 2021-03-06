package PropMap
 
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


object compare_sparksql { 
  def main(args: Array[String]) { 
     //屏蔽日志
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);
    
    //val sparkConf = new SparkConf().setAppName("spark2SQL")
    val warehouseLocation = "spark-warehouse"
    
    val ss = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.schema.verification", false)
      .enableHiveSupport()
      .getOrCreate()
  
    import ss.implicits._
    import ss.sql
    
    var transdata = sql(s"select trans_at,orig_trans_at " +
        s"from tbl_common_his_trans where pdate=20160701").cache
    
        
    println("transdata count: " + transdata.count())
   
    println("Equal count: " + transdata.filter("trans_at = orig_trans_at").count())
    println("More count: " + transdata.filter("trans_at > orig_trans_at").count())
    println("Less count: " + transdata.filter("trans_at < orig_trans_at").count())
    println("orig_trans_at 0 count: " + transdata.filter("orig_trans_at=0").count())    
    println("Done")
    
 }
}