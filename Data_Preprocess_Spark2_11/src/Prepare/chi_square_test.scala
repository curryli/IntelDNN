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
//import org.apache.spark.mllib.stat.Statistics

import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.feature.ChiSqSelectorModel

object chi_square_test {
 
  val NAN_Arr = IntelUtil.constUtil.NAN_Arr 

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
    var labeledData = IntelUtil.get_from_HDFS.get_labeled_DF(ss, input_dir).persist(StorageLevel.MEMORY_AND_DISK_SER)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    labeledData.show(10)
    
     
    val no_idx_arr = labeledData.columns.slice(0,6)
    
    val Arr_to_idx = labeledData.columns.toList.drop(6).toArray   ///.dropRight(1)
    
    
    val CatVecArr = Arr_to_idx.map { x => x + "_idx"}
     //CatVecArr.foreach {println }
     
    val labeled_arr = no_idx_arr.++(CatVecArr)
    labeled_arr.foreach {println }
    
    val pipeline_idx = new Pipeline().setStages(IntelUtil.funUtil.Multi_idx_Pipeline(Arr_to_idx).toArray)

    labeledData = pipeline_idx.fit(labeledData).transform(labeledData)
    println("idx done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
    labeledData.show(5)
    
  
    labeledData.selectExpr(labeled_arr:_*).rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "idx_withlabel")
    
    val assembler = new VectorAssembler()
      .setInputCols(CatVecArr)
      .setOutputCol("featureVector")
      
    labeledData = assembler.transform(labeledData)  
    labeledData.show(5)  
    

    
    val st = new ChiSqSelector()  
                .setFeaturesCol("featureVector")  
                .setLabelCol("label_filled_idx")  
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