package PropMap
 
import IntelUtil._
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


object read_HDFS { 
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
      .appName("Spark Hive")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.schema.verification", false)
      .getOrCreate()
  
    import ss.implicits._
    val sc = ss.sparkContext
    
//    val file_0701 = sc.textFile("/user/hddtmn/in_common_his_trans/20160701_correct")//.persist(StorageLevel.MEMORY_AND_DISK_SER)
//    //val RDD_0701 = file_0701.map(str=> Row.fromSeq(str.split("\",\"")))
//    
//    val RDD_0701 = file_0701.map{str=>
//      var tmparr = str.split("\",\"")          //这里忽略了第一个和最后一个" 对我们建模没有影响  
//      tmparr = tmparr.map { x => x.toString()}    //这里tmparr长度是251，所以用到的schema长度也需要时251，否则报错
//      Row.fromSeq(tmparr.toSeq)
//    }
//     
//    val DF_0701 = ss.createDataFrame(RDD_0701, constUtil.schema_251)
//    DF_0701.show
  
    
    var All_DF: DataFrame = null
    //until和Range是左闭右开，1是包含的，10是不包含。而to是左右都包含。  for(i <- 0 until 10);  var r = Range(1,10,2);  默认步长1
    for(i <- 1 to 5){         
       val filename = "/user/hddtmn/in_common_his_trans/" + constUtil.dateMap(i) + "_correct"
       println(filename)
       val tmpRdd = sc.textFile(filename).map{str=>
           var tmparr = str.split("\",\"")         
           tmparr = tmparr.map { x => x.toString()}    
           Row.fromSeq(tmparr.toSeq)
       }
   
       var tmp_DF = ss.createDataFrame(tmpRdd, constUtil.schema_251)
    
       val udf_pdate = udf[String, String]{xstr => constUtil.dateMap(i)}
       tmp_DF = tmp_DF.withColumn("pdate", udf_pdate(tmp_DF("pri_key")))
       tmp_DF.show(5)
       if(i==1)
         All_DF = tmp_DF
       else
         All_DF = All_DF.unionAll(tmp_DF)
    }
    
    All_DF.show(10)
    
     
    println("Done")
    
 }
}