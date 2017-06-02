package PropMap

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import scala.collection.mutable.MutableList
import scala.Range
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.feature.QuantileDiscretizer

object WOE {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);
    
   val conf = new SparkConf().setAppName("compare_2time")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
    
   val startTime = System.currentTimeMillis(); 
   
   var transdata = hc.sql(s"select cast(tfr_dt_tm as double), cast(trans_at as  double), cast(total_disc_at as double) "+
	    s"from tbl_common_his_trans where pdate=20160701 and substring(acpt_ins_id_cd,5,4)=3940 ").cache
	    
	 
	 val transtime_splits = Array(0.0, 701010000.0, 701020000.0, 701030000.0, 701040000.0, 701050000.0, 701060000.0,
	                                         701070000.0, 701080000, 701090000.0, 701100000.0, 701110000.0, 702000000.0)

	 val transtime_bucketizer = new Bucketizer()
          .setInputCol("tfr_dt_tm")
          .setOutputCol("bucketed_time")
          .setSplits(transtime_splits)   
	    
	 val totalMoney_splits = Array(0,10,100,1000,10000,100000,1000000,10000000,100000000,Double.PositiveInfinity)

	 val totalMoney_bucketizer = new Bucketizer()
          .setInputCol("trans_at")
          .setOutputCol("bucketed_money")
          .setSplits(totalMoney_splits)     
	  
	    
	 val bucketedData = totalMoney_bucketizer.transform((transtime_bucketizer.transform(transdata)))
	    
	 bucketedData.show(5)    
	 println("bucketedData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )     
	    
	    
//	   val transtime_QD = new QuantileDiscretizer()          
//             .setInputCol("tfr_dt_tm")
//             .setOutputCol("QD_time")
//             .setNumBuckets(12)//设置分箱数,不用像Bucketizer那样指定分配区间，用 QuantileDiscretizer分出来的每一个区间内的数量基本是平均分的
//             
//     val totalMoney_QD = new QuantileDiscretizer()
//             .setInputCol("trans_at")
//             .setOutputCol("QD_money")
//             .setNumBuckets(15)//设置分箱数
//        
//     val QD_Data_1 = transtime_QD.fit(transdata).transform(transdata)
//     println("QD_Data_1 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
//     
//     
//     val QD_Data_2 = totalMoney_QD.fit(QD_Data_1).transform(QD_Data_1)
//     
//     QD_Data_2.show(5)
//     
//     println("QD_Data_2 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
	 
	 
      val QD_Data_1 = discretizerFun("tfr_dt_tm", 12).fit(transdata).transform(transdata)	 
      val QD_Data_2 = discretizerFun("trans_at", 15).fit(QD_Data_1).transform(QD_Data_1)	
      
      QD_Data_2.show(5)
      println("QD_Data_2 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
	 
   }
  
  
  
  def discretizerFun (col: String, bucketNo: Int): QuantileDiscretizer = {
       val discretizer = new QuantileDiscretizer()
         discretizer.setInputCol(col)
                    .setOutputCol(s"${col}_QD")
                    .setNumBuckets(bucketNo)
   }

  
   
   
}