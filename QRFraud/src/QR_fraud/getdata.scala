package QR_fraud
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

 


object getdata {
  def main(args: Array[String]): Unit = {
  }
  
 
  def get_from_hive(hc: HiveContext):DataFrame = {      
   val startTime = System.currentTimeMillis(); 
     
   var data_train =  hc.sql(s"select * from xrlidb.used_train")
   
   var data_test =  hc.sql(s"select * from xrlidb.used_test")
   
   var black_trans =  hc.sql(s"select * from xrlidb.black_trans").cache
   
   
   
   var fraud_train = hc.sql(s"select A.* from xrlidb.used_train A left join xrlidb.black_trans B on A.sys_tra_no = B.sys_tra_no and A.pdate = B.pdate where B.acct_class is not null").cache
   var fraud_test = hc.sql(s"select A.* from xrlidb.used_test A left join xrlidb.black_trans B on A.sys_tra_no = B.sys_tra_no and A.pdate = B.pdate where B.acct_class is not null").cache
   
   
/////////////////////////////   
   var black_tmp = black_trans.select("sys_tra_no", "mchnt_tp").withColumnRenamed("sys_tra_no","b_sys").withColumnRenamed("mchnt_tp","b_mchnt_tp")

   var train_join = data_train.join(black_tmp, data_train("sys_tra_no")===black_tmp("b_sys"), "left_outer") 
   var test_join = data_test.join(black_tmp, data_test("sys_tra_no")===black_tmp("b_sys"), "left_outer") 
   
   var normal_train = train_join.filter(train_join("b_mchnt_tp").isNull).drop("b_sys").drop("b_mchnt_tp")
   var normal_test = test_join.filter(test_join("b_mchnt_tp").isNull).drop("b_sys").drop("b_mchnt_tp")
   
   black_trans.unpersist(blocking=false)
   
///////////////////////////// 
   
   
//   val black_sysno_train = fraud_train.select("sys_tra_no").distinct().map(r=>r.getString(0)).collect()
//   val black_sysno_test = fraud_test.select("sys_tra_no").distinct().map(r=>r.getString(0)).collect()
//   
//   var normal_train = data_train.filter(!data_train("sys_tra_no").isin(black_sysno_train))
//   var normal_test = data_train.filter(!data_train("sys_tra_no").isin(black_sysno_test))

//   val udf_Map0 = udf[Double, String]{xstr => 0.0}
//   val udf_Map1 = udf[Double, String]{xstr => 1.0}
   
   
   val divide =  (col: String, norm_or_fraud: String, train_or_test: String) => {
      norm_or_fraud + "_" + train_or_test
    }
   
   val udf_divide = udf(divide)
   
   val set_label =  (col: String, norm_or_fraud: Double) => {
      norm_or_fraud
    }
   
   val udf_label = udf(set_label)  
   
   var normal_train_lb = normal_train.withColumn("division", udf_divide(normal_train("area_cd"), lit("normal"), lit("train"))).withColumn("label", udf_label(normal_train("area_cd"), lit(1.0)))
   var normal_test_lb = normal_test.withColumn("division", udf_divide(normal_test("area_cd"), lit("normal"), lit("test"))).withColumn("label", udf_label(normal_test("area_cd"), lit(1.0)))
   var fraud_train_lb = fraud_train.withColumn("division", udf_divide(fraud_train("area_cd"), lit("fraud"), lit("train"))).withColumn("label", udf_label(fraud_train("area_cd"), lit(0.0)))
   var fraud_test_lb = fraud_test.withColumn("division", udf_divide(fraud_test("area_cd"), lit("fraud"), lit("test"))).withColumn("label", udf_label(fraud_test("area_cd"), lit(0.0)))
    
   
   println(" normal_train_lb count: " + normal_train_lb.count +  
           " normal_test_lb count: " + normal_test_lb.count + 
           " fraud_train_lb count: " + fraud_train_lb.count + 
           " fraud_test_lb count: " + fraud_test_lb.count )
           
 
           
           
//   var labeled_train = normal_train_lb.unionAll(fraud_train_lb)
//   var labeled_test = normal_test_lb.unionAll(fraud_test_lb)
    
   var data_division = normal_train_lb.unionAll(fraud_train_lb).unionAll(normal_test_lb).unionAll(fraud_test_lb)
    
   println("data_division done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
     
   data_division
  }
   
  
}