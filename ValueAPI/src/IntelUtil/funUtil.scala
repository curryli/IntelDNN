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
 

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import scala.collection.mutable.Map

object funUtil { 
  
  def main(args: Array[String]) { 
    println(dayForWeek("20161001"))
     
    val start = "2014-01-03";  
    val end = "2014-03-05";  
    val sdf = new SimpleDateFormat("yyyy-MM-dd");  
    val dBegin = sdf.parse(start);  
    val dEnd = sdf.parse(end);  
    val listDate = getDatesBetweenTwoDate(dBegin, dEnd); 
    
    for(i <- 0 to listDate.size()-1){ 
       println(sdf.format(listDate.get(i)));  
    }  
     
    
     val start2 = "20140103";  
        val end2 = "20140305";   
        val DateMap = createDateMap(start2, end2); 
        
        for(i <- 0 to DateMap.size()-1){ 
           println(DateMap.get(i)); 
        }
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
 
 
   
   def getDeltaTime(start: String, end: String, interval: String): Double={
		  val format = new SimpleDateFormat("MMddHHmmss")
			
		  try {
  		  var start_t = format.parse(start) 
  			var end_t = format.parse(end) 
  			val delta_ms = (end_t.getTime()-start_t.getTime()).toDouble
  			
  			val delta_seconds = delta_ms/1000
  			val delta_mins = delta_seconds/60
  			val delta_hours = delta_mins/60
  			 
  			var result =
         interval match {
           case "seconds"   => delta_seconds
           case "mins"   => delta_mins
           case "hours"    => delta_hours
        }
  			return result 
		  }
		  
		  catch {
         case ex: ParseException =>{
            println("ParseException")
            
          return Double.NaN
         }
		  }
	}
			
 
 
  def get_schema(arr: Array[String]): StructType ={
    var tmp_schema = new StructType()
    for(item<-arr){
      tmp_schema = tmp_schema.add(item,StringType,true)
    }
    tmp_schema
  }
   

  def getDatesBetweenTwoDate(beginDate:Date, endDate:Date):ArrayList[Date] ={  
        val lDate = new ArrayList[Date]();  
        lDate.add(beginDate);// 把开始时间加入集合  
        val cal = Calendar.getInstance();  
        // 使用给定的 Date 设置此 Calendar 的时间  
        cal.setTime(beginDate);  
        var bContinue = true;  
        while (bContinue) {  
            // 根据日历的规则，为给定的日历字段添加或减去指定的时间量  
            cal.add(Calendar.DAY_OF_MONTH, 1);  
            // 测试此日期是否在指定日期之后  
            if (endDate.after(cal.getTime())) {  
                lDate.add(cal.getTime());  
            } else {  
                bContinue=false;  
            }  
        }  
        lDate.add(endDate);// 把结束时间加入集合  
        return lDate;  
    } 
  
  
  
     def createDateMap(beginDate:String, endDate:String):ArrayList[String] ={  
        val sdf = new SimpleDateFormat("yyyyMMdd");  
        val lDate = new ArrayList[String]();  
        lDate.add(beginDate);// 把开始时间加入集合  
        val cal = Calendar.getInstance();  
        // 使用给定的 Date 设置此 Calendar 的时间  
        
        cal.setTime(sdf.parse(beginDate));  
        var bContinue = true;  
        while (bContinue) {  
            // 根据日历的规则，为给定的日历字段添加或减去指定的时间量  
            cal.add(Calendar.DAY_OF_MONTH, 1);  
            // 测试此日期是否在指定日期之后  
            if (sdf.parse(endDate).after(cal.getTime())) {  
                lDate.add(sdf.format(cal.getTime()));  
            } else {  
                bContinue=false;  
            }  
        }  
        lDate.add(endDate);// 把结束时间加入集合  
        return lDate;   
     }
     
     
     
     
     def Date_idx_Map(beginDate:String, endDate:String):Map[String,Int] ={  
        val sdf = new SimpleDateFormat("yyyyMMdd") 
        val lDate =  Map[String,Int]()
        var i = 0 
        lDate.+=(beginDate->i)
        val cal = Calendar.getInstance()  
    
        cal.setTime(sdf.parse(beginDate))  
        var bContinue = true 
        while (bContinue) {  
            i=i+1
            cal.add(Calendar.DAY_OF_MONTH, 1) 
            if (sdf.parse(endDate).after(cal.getTime())) {  
                lDate.+=(sdf.format(cal.getTime())->i);  
            } else {  
                bContinue=false;  
            }  
        }  
        lDate.+=(endDate->i);// 把结束时间加入集合  
        return lDate;  
    } 
     
     
     def idx_Date_Map(beginDate:String, endDate:String):Map[Int,String] ={  
        val sdf = new SimpleDateFormat("yyyyMMdd") 
        val lDate =  Map[Int,String]()
        var i = 0 
        lDate.+=(i->beginDate)
        val cal = Calendar.getInstance()  
    
        cal.setTime(sdf.parse(beginDate))  
        var bContinue = true 
        while (bContinue) {  
            i=i+1
            cal.add(Calendar.DAY_OF_MONTH, 1) 
            if (sdf.parse(endDate).after(cal.getTime())) {  
                lDate.+=(i->sdf.format(cal.getTime()));  
            } else {  
                bContinue=false;  
            }  
        }  
        lDate.+=(i->endDate);// 把结束时间加入集合  
        return lDate;  
    } 
     
  
}