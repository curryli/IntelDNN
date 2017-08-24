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
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.ChiSqSelector
 

object chi_square_3 {
 

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
     
    var input_dir = rangedir + "idx_withlabel"
    var labeledData = IntelUtil.get_from_HDFS.get_indexed_DF(ss, input_dir).persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    labeledData.show(10)
     
    val no_idx_arr = labeledData.columns.slice(0,6)
    val Arr_to_idx = labeledData.columns.toList.drop(6).toArray   ///.dropRight(1)
  
    val assembler = new VectorAssembler()
      .setInputCols(Arr_to_idx)
      .setOutputCol("featureVector")
      
    labeledData = assembler.transform(labeledData)  
    labeledData.show(5)  
    

    
    val st = new ChiSqSelector()  
                .setFeaturesCol("featureVector")  
                .setLabelCol("label")  
                .setOutputCol("selectedFeatures")
                .setFpr(0.05)
                //.setNumTopFeatures(2)  
                 
     val model = st.fit(labeledData)
 
    // 计算
     val result = model.transform(labeledData)

    println(s"ChiSqSelector output with top ${st.getNumTopFeatures} features selected")
    result.show(5)
 
    
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
  }
  
  

    
}