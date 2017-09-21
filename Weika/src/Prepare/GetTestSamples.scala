package Prepare
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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import scala.collection.mutable.MutableList
import scala.Range
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.QuantileDiscretizer
import scala.collection.mutable.HashMap


object GetTestSamples {
 
    val startdate = IntelUtil.varUtil.startdate
    val enddate = IntelUtil.varUtil.enddate
    val rangedir = IntelUtil.varUtil.rangeDir 
    val usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
     
    val fraudType = "04"
    
      
   //var idx_modelname = IntelUtil.varUtil.idx_model
   var idx_modelname = IntelUtil.varUtil.idx_model
   
   
  def main(args: Array[String]): Unit = {

    //屏蔽日志
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.ERROR);
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
 
     
    var input_dir = rangedir + "Labeled_All"
     
    var fraud_join_Data = IntelUtil.get_from_HDFS.get_fraud_join_DF(ss, startdate, enddate).persist(StorageLevel.MEMORY_AND_DISK_SER)
    var fraudType_infraud = fraud_join_Data.filter(fraud_join_Data("fraud_tp")=== fraudType) 
    var allfraud_cards = fraud_join_Data.select("pri_acct_no_conv").distinct().persist(StorageLevel.MEMORY_AND_DISK_SER) 
    var allfraud_cards_list = allfraud_cards.rdd.map(r=>r.getString(0)).collect()
    println(1)
    
    
    var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000) 
    var Normal_data = AllData.filter(!AllData("pri_acct_no_conv_filled").isin(allfraud_cards_list:_*))
    println(2)
    var Normal_filled = Normal_data.selectExpr(usedArr_filled:_*)
    println(3)
    
    for(col<-usedArr_filled)
       Normal_filled = Normal_filled.withColumnRenamed(col, col.substring(0, col.length-7) )
    
       println(4)
    var fraudType_fraud = AllData.join(fraudType_infraud, AllData("sys_tra_no")===fraudType_infraud("sys_tra_no"), "leftsemi")
    var fraudType_filled = fraudType_fraud.selectExpr(usedArr_filled:_*)
    for(col<-usedArr_filled)
      fraudType_filled = fraudType_filled.withColumnRenamed(col, col.substring(0, col.length-7) )
   
      println(5)
      
    val udf_Map0 = udf[Double, String]{xstr => 0.0}
    val udf_Map1 = udf[Double, String]{xstr => 1.0}
         
    var NormalData_labeled = Normal_filled.withColumn("label", udf_Map0(Normal_filled("trans_md")))
    var fraudType_labeled = fraudType_filled.withColumn("label", udf_Map1(fraudType_filled("trans_md")))
    var labeledData = fraudType_labeled.unionAll(NormalData_labeled)
    
    labeledData.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "labeledData_tmp")
    
    println("get labeledData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
      
    labeledData = Prepare.FeatureEngineer_function.FE_function(ss, labeledData)
     
    val getdate = udf[Long, String]{xstr => xstr.substring(0,4).toLong}
    labeledData = labeledData.filter(getdate(labeledData("tfr_dt_tm").===(enddate)))
    
    labeledData.count()
    labeledData.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "labeledData_enddate")
    
    println("labeledData in one day obtained, and total count is: ", labeledData.count())  
    
    
    var All_cols =  labeledData.columns
    
    var Arr_to_idx = IntelUtil.constUtil.DisperseArr
    
    val no_idx_arr = All_cols.toSet.diff(Arr_to_idx.+:("pri_acct_no_conv").+:("label").toSet).toList
    

    val CatVecArr = Arr_to_idx.map { x => x + "_idx"}
    //CatVecArr.foreach {println }
     
 
    
    val my_index_Model = PipelineModel.load(idx_modelname)
   
    labeledData = my_index_Model.transform(labeledData)
      
    println("Index pipeline done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
    val feature_arr = no_idx_arr.++(CatVecArr)      //加载idx_model时使用
    println("feature_arr: ", feature_arr.mkString(","))
    
    labeledData = labeledData.selectExpr(List("pri_acct_no_conv","label").++(feature_arr):_*) 

    //labeledData.dtypes.foreach(println)
    labeledData.show(5)
     //该数组李必须都是doubletype，否则VectorAssembler报错 
     
   // val feature_arr = no_idx_arr.++(CatVecArr)    //不加载idx_model时使用
    
     
    println("start change to double")

    var db_list = List("pri_acct_no_conv","label")
    
     for(col <- feature_arr){
        val newcol = col + "_db" 
        db_list = db_list.:+(newcol)
        labeledData = labeledData.withColumn(newcol, labeledData.col(col).cast(DoubleType))
     }
 
    println("change to double done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    //labeledData.dtypes.foreach(println)
    //db_list.foreach(println)
        
    labeledData = labeledData.na.fill(-1.0)   // 因为这里填的是-1.0，是double类型，所以  好像只能对double类型的列起作用。  如果填充“1”，那么只对String类型起作用。
    //labeledData.show()
    
    labeledData = labeledData.selectExpr(db_list:_*).persist(StorageLevel.MEMORY_AND_DISK_SER)
    labeledData.show(5)
    
    labeledData.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "FE_db_enddate")
    println("Saved FE_db done:   ",labeledData.columns.mkString(","))
     
     
     
  }
  
  
  
    
}