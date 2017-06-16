package PropMap
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


object save_labeled_byday {
  

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

    var usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
    
    var counterfeit_cards:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = null
    var save_counterfeit_filled_DF:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = null
    var save_labeled_DF:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = null
    
    val dst_date = "20160702"
    val dst_index = IntelUtil.constUtil.date_to_num_Map(dst_date)
    
    val start_date = "20160701"
    var cur_date = start_date
    var cur_index = IntelUtil.constUtil.date_to_num_Map(cur_date)
    
    while(cur_index <= dst_index){
        cur_date = IntelUtil.constUtil.dateMap(cur_index)
        var fraud_join_Data = IntelUtil.get_from_HDFS.get_fraud_join_DF_byday(ss, cur_date).persist(StorageLevel.MEMORY_AND_DISK_SER)
        //fraud_join_Data.show(5)
        println("fraud_join_Data done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
          
        var counterfeit_infraud = fraud_join_Data.filter(fraud_join_Data("fraud_tp")==="04") 
        
        var tmp_counterfeit_cards = counterfeit_infraud.select("pri_acct_no_conv").distinct().persist(StorageLevel.MEMORY_AND_DISK_SER) 
        
        if(cur_index.equals(IntelUtil.constUtil.date_to_num_Map(start_date)))
          counterfeit_cards = tmp_counterfeit_cards
        else
          counterfeit_cards = counterfeit_cards.union(tmp_counterfeit_cards)
         
    	  fraud_join_Data.unpersist(false)
    	  
    	  var counterfeit_cards_list = counterfeit_cards.rdd.map(r=>r.getString(0)).collect()
    	
        var AllData = IntelUtil.get_from_HDFS.get_filled_DF_byday(ss, cur_date).repartition(1000).persist(StorageLevel.MEMORY_AND_DISK_SER) 
        println("AllData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
        
        val counterfeit_related = AllData.filter(AllData("pri_acct_no_conv").isin(counterfeit_cards_list:_*))
        println("counterfeit_related done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
        
        var counterfeit_fraud = counterfeit_related.join(counterfeit_infraud, counterfeit_related("sys_tra_no")===counterfeit_infraud("sys_tra_no"), "leftsemi")
        
        val counterfeit_filled = counterfeit_fraud.selectExpr(usedArr_filled:_*)
        println("counterfeit_filled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
        
        var counterfeit_related_normal =  counterfeit_related.selectExpr(usedArr_filled:_*).except(counterfeit_filled)
        println("counterfeit_related_normal done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )                             
         
        
        if(cur_index.equals(IntelUtil.constUtil.date_to_num_Map(start_date)))
          save_counterfeit_filled_DF = counterfeit_filled
        else  
          save_counterfeit_filled_DF = save_counterfeit_filled_DF.union(counterfeit_filled)
    
        ////////////////////////////////////////////////Normal////////////////////////
        
        var sample_cards = AllData.sample(false, 0.000002, 0).select("pri_acct_no_conv").distinct()
        var normal_cards = sample_cards.except(counterfeit_cards) //不准确 ，最好是 fraud_cards_list，不过概率很小，差不多
        println("normal_cards done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )                             
        
        var normal_cards_list = normal_cards.rdd.map(r=>r.getString(0)).collect()
 
        val normal_cards_data = AllData.filter(AllData("pri_acct_no_conv").isin(normal_cards_list:_*))
        
        counterfeit_cards.unpersist(false)
		    AllData.unpersist(false)
		    
        println("normal_cards_data count is " + normal_cards_data.count())
        println("normal_cards_data done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
        //normal_cards.show(5)
        
        val normal_data_filled = normal_cards_data.selectExpr(usedArr_filled:_*).unionAll(counterfeit_related_normal)
        //normal_data.show(5)
        println("normal_data_filled done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
        
        val udf_Map0 = udf[Double, String]{xstr => 0.0}
        val udf_Map1 = udf[Double, String]{xstr => 1.0}
        
        var NormalData_labeled = normal_data_filled.withColumn("isFraud", udf_Map0(normal_data_filled("trans_md_filled")))
        var counterfeit_labeled = counterfeit_filled.withColumn("isFraud", udf_Map1(counterfeit_filled("trans_md_filled")))
        var LabeledData = counterfeit_labeled.union(NormalData_labeled)
        
        if(cur_index.equals(IntelUtil.constUtil.date_to_num_Map(start_date)))
          save_labeled_DF = LabeledData
        else 
          save_labeled_DF = save_labeled_DF.union(LabeledData)
         
        cur_index = cur_index + 1
        println("###################################################################################################")
        println("cur_index " + cur_index + " done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
    }
    
    
     counterfeit_cards.rdd.map(r=>r.getString(0)).saveAsTextFile("xrli/IntelDNN/counterfeit_cards_for07")
     save_counterfeit_filled_DF.rdd.map(_.mkString(",")).saveAsTextFile("xrli/IntelDNN/counterfeit_filled_for07")
     save_labeled_DF.rdd.map(_.mkString(",")).coalesce(1).saveAsTextFile("xrli/IntelDNN/Labeled_filled_for07")
    
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
    
    
  }
  
    
}