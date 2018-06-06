package split
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions 

import org.apache.spark.sql.expressions._

import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import scala.reflect.ClassTag

  
object saveIndex {
  
   def any_to_double[T: ClassTag](b: T):Double={
    if(b==true)
      1.0
    else
      0
  }
  
   
  val udf_bool_to_double = udf[Double, Boolean]{xstr => any_to_double(xstr)} 
  val udf_int_to_double = udf[Double, Int]{xstr => any_to_double(xstr)}  
  val get_day_week = udf[Int, String]{xstr => IntelUtil.funUtil.dayForWeek(xstr)}
  
  var index_arr = Array[String]()
  
  
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);

    //    require(args.length == 3)

    val conf = new SparkConf().setAppName("QR_fraud")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
    
    
    val startTime = System.currentTimeMillis(); 
     
   var data_train =  hc.sql(s"select * from xrlidb.used_train").repartition(100)
   
   var data_test =  hc.sql(s"select * from xrlidb.used_test").repartition(100)
   
   var black_trans =  hc.sql(s"select * from xrlidb.black_trans").cache()//.persist(StorageLevel.MEMORY_AND_DISK_SER)
   
   
   
   var fraud_train = hc.sql(s"select A.* from xrlidb.used_train A left join xrlidb.black_trans B on A.sys_tra_no = B.sys_tra_no and A.pdate = B.pdate where B.acct_class is not null").cache()//.persist(StorageLevel.MEMORY_AND_DISK_SER)
   var fraud_test = hc.sql(s"select A.* from xrlidb.used_test A left join xrlidb.black_trans B on A.sys_tra_no = B.sys_tra_no and A.pdate = B.pdate where B.acct_class is not null").cache()//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
/////////////////////////////   
   var black_tmp = black_trans.select("sys_tra_no", "mchnt_tp").withColumnRenamed("sys_tra_no","b_sys").withColumnRenamed("mchnt_tp","b_mchnt_tp")

   var train_join = data_train.join(black_tmp, data_train("sys_tra_no")===black_tmp("b_sys"), "left_outer") 
   var test_join = data_test.join(black_tmp, data_test("sys_tra_no")===black_tmp("b_sys"), "left_outer") 
   
   var normal_train = train_join.filter(train_join("b_mchnt_tp").isNull).drop("b_sys").drop("b_mchnt_tp")
   var normal_test = test_join.filter(test_join("b_mchnt_tp").isNull).drop("b_sys").drop("b_mchnt_tp")
   
   black_trans.unpersist(blocking=false)
    
   val divide =  (col: String, norm_or_fraud: String, train_or_test: String) => {
      norm_or_fraud + "_" + train_or_test
    }
   
   val udf_divide = udf(divide)
   
   val set_label =  (col: String, norm_or_fraud: Double) => {
      norm_or_fraud
    }
   
   val udf_label = udf(set_label)  
   
   var normal_train_lb = normal_train.withColumn("division", udf_divide(normal_train("area_cd"), lit("normal"), lit("train"))).withColumn("label", udf_label(normal_train("area_cd"), lit(0.0))).repartition(100)
          //.sample(false, 0.001)
   var normal_test_lb = normal_test.withColumn("division", udf_divide(normal_test("area_cd"), lit("normal"), lit("test"))).withColumn("label", udf_label(normal_test("area_cd"), lit(0.0))).repartition(100)
         //.sample(false, 0.001)
   var fraud_train_lb = fraud_train.withColumn("division", udf_divide(fraud_train("area_cd"), lit("fraud"), lit("train"))).withColumn("label", udf_label(fraud_train("area_cd"), lit(1.0))).repartition(100)
   var fraud_test_lb = fraud_test.withColumn("division", udf_divide(fraud_test("area_cd"), lit("fraud"), lit("test"))).withColumn("label", udf_label(fraud_test("area_cd"), lit(1.0))).repartition(100)
    
   
   println(" normal_train_lb count: " + normal_train_lb.count +  
           " normal_test_lb count: " + normal_test_lb.count + 
           " fraud_train_lb count: " + fraud_train_lb.count + 
           " fraud_test_lb count: " + fraud_test_lb.count )
           
   
   fraud_train.unpersist(blocking=false)        
   fraud_test.unpersist(blocking=false)
           
           
//   var labeled_train = normal_train_lb.unionAll(fraud_train_lb)
//   var labeled_test = normal_test_lb.unionAll(fraud_test_lb)
    
   var data_division = normal_train_lb.unionAll(fraud_train_lb).unionAll(normal_test_lb).unionAll(fraud_test_lb).repartition(100).cache
    
   println("data_division done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
   
   
   //////////////////////////////////////////////
   var DisperseArr = IntelUtil.varUtil.DisperseArr
         
    data_division = data_division.na.fill("NULL",DisperseArr);   // null和empty不是一回事
    
    val udf_replaceEmpty = udf[String, String]{xstr => 
        if(xstr.isEmpty())
          "NANs"
        else
          xstr
      }
 
    
   for(oldcol <- DisperseArr){   
        val newcol = oldcol + "_filled" 
        val col_cnt = data_division.select(oldcol).distinct().count()

        println(oldcol , " count: ", col_cnt)

        data_division = data_division.withColumn(newcol, udf_replaceEmpty(data_division(oldcol)))

        var indexCat = oldcol + "_CatVec"
        
        index_arr = index_arr.+:(indexCat)
        
        var indexer = new StringIndexer().setInputCol(newcol).setOutputCol(indexCat).setHandleInvalid("skip")
        data_division = indexer.fit(data_division).transform(data_division)

      }
       
     
    println("data_division index done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
             
    data_division.show(10) 
    
    val used_arr = IntelUtil.varUtil.ori_sus_Arr.++(index_arr).+:("label").+:("division").+:("pri_acct_no")
    
    
    println(used_arr.mkString(","))
    
    data_division.selectExpr(used_arr:_*).rdd.map(_.mkString(",")).saveAsTextFile("xrli/QRfraud/saveIndex")
  
  }
  
  
  
}