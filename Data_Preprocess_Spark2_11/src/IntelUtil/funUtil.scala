package IntelUtil

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
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.HashMap

object funUtil { 
  def main(args: Array[String]) { 
    println(dayForWeek("20161001"))
  }
		 
	
  def getPipeline(DisperseArr: Array[String]): ArrayBuffer[PipelineStage] = {
    val pipelineStages = new ArrayBuffer[PipelineStage]
    var i=0
    for (col <- DisperseArr) {
      i=i+1
      println(i)
      pipelineStages += new StringIndexer()
        .setInputCol(col + "_filled")
        .setOutputCol(col + "_CatVec")
        .setHandleInvalid("skip")
    }
    
    pipelineStages
  }
  
  
 def dayForWeek(pTime: String): Int={  
     //val format = new SimpleDateFormat("yyyy-MM-dd")
     val format = new SimpleDateFormat("yyyyMMdd")
     var c = Calendar.getInstance()
     c.setTime(format.parse(pTime)) 
     var dayForWeek = 0;  
     if(c.get(Calendar.DAY_OF_WEEK) == 1)
       dayForWeek = 7
     else  
      dayForWeek = c.get(Calendar.DAY_OF_WEEK) - 1
      
     return dayForWeek;  
 }
			
 
 def get_woe_map(vec_data:DataFrame, colname:String, label_col:String="label_filled"): HashMap[Double,Double]={
     var bad_all = vec_data.filter(vec_data(label_col)==="1.0") 
     var good_all = vec_data.filter(vec_data(label_col)==="0.0") 
     var bad_count_all = bad_all.count()
     var good_count_all = good_all.count()
     println("bad_count_all count is: " +  bad_count_all)
     println("good_count_all count is: " +  good_count_all)
      
     val woeMap = new HashMap[Double,Double]()
     
     //val QD_num = QD_money_num
     val cat_set = vec_data.select(colname).rdd.map(x=>x.getDouble(0)).distinct().collect
     println("cat_set is : ")
     cat_set.foreach{println}
     
     for(i<-cat_set){
        var bad_i = bad_all.filter(bad_all(colname)===i.toDouble) 
        var bad_count_i = bad_i.count()
        println("bad_count_i " + i +  " count is: " +  bad_count_i)   
        
        var good_i = good_all.filter(good_all(colname)===i.toDouble) 
        var good_count_i = good_i.count()
        println("good_count_i " + i +  " count is: " +  good_count_i)   
      
        var woe_i = Math.log((bad_count_i.toDouble/bad_count_all.toDouble)/(good_count_i.toDouble/good_count_all.toDouble))     
        //var woe_i = Math.log((bad_count_i/bad_count_all)*(good_count_all/good_count_i))
        
        woeMap.+=(i.toDouble -> woe_i)
     }
     
     woeMap 
  }
  
  def WOE_modify(vec_data:DataFrame, colname:String, woeMap: HashMap[Double, Double]):DataFrame={
    val map_name = "woeMap_" + colname
 
    val newcol_name = colname + "_WOE"
    val udf_woemap = udf[Double, Double]{key => woeMap(key)}
    
    val modified_vec = vec_data.withColumn(newcol_name, udf_woemap(vec_data(colname)))
    
    modified_vec
  }
  
}