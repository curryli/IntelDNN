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


object LabelFlow {
    val startdate = IntelUtil.varUtil.startdate
    val enddate = IntelUtil.varUtil.enddate
    val rangedir = IntelUtil.varUtil.rangeDir 
    val usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
    
    //可以调整
    val TF_ratio = 10
     
    
    
    var counterfeit_cards_num = 0L
    var normal_cards_num = 0L
    var counterfeit_related_fraud_count = 0L
     
 
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
    
    save_counterfeit_1(ss)
    println("counterfeit_cards count is " + counterfeit_cards_num)
    println("step1 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
      
    save_sample_cards_2(ss)
    println("step2 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
    save_Alldata_bycards_3(ss)
    println("step3 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
    save_labeled_new_4(ss)
    println("step4 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
  }
    
    
    def save_counterfeit_1(ss: SparkSession): Unit ={
        var fraud_join_Data = IntelUtil.get_from_HDFS.get_fraud_join_DF(ss, startdate, enddate).persist(StorageLevel.MEMORY_AND_DISK_SER)
         
        var counterfeit_infraud = fraud_join_Data.filter(fraud_join_Data("fraud_tp")==="04") 
        
        var counterfeit_cards = counterfeit_infraud.select("pri_acct_no_conv").distinct().persist(StorageLevel.MEMORY_AND_DISK_SER) 
        counterfeit_cards_num = counterfeit_cards.count()
        //AllFlow.counterfeit_cards_num = counterfeit_cards.count()
     
        counterfeit_cards.rdd.map(r=>r.getString(0)).saveAsTextFile(rangedir + "counterfeit_cards")
     
    	  fraud_join_Data.unpersist(false)
    	  
    	  var counterfeit_cards_list = counterfeit_cards.rdd.map(r=>r.getString(0)).collect()
    	
        var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000) 
         
        val counterfeit_related = AllData.filter(AllData("pri_acct_no_conv").isin(counterfeit_cards_list:_*))
        println("counterfeit_related_all_data count is " + counterfeit_related.count()) 
         
        var counterfeit_fraud = counterfeit_related.join(counterfeit_infraud, counterfeit_related("sys_tra_no")===counterfeit_infraud("sys_tra_no"), "leftsemi")
        
        val counterfeit_filled = counterfeit_fraud.selectExpr(usedArr_filled:_*)
        counterfeit_related_fraud_count = counterfeit_filled.count()
        println("counterfeit_related_fraud_data count is " + counterfeit_related_fraud_count)
         
        counterfeit_filled.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "counterfeit_filled")
    }
    
    
    def save_sample_cards_2(ss: SparkSession, sample_ratio: Double = 0.00001): Unit ={
        var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000) 
        var tmp_ratio = counterfeit_related_fraud_count.toDouble/80000000.toDouble
        var Ratio =  tmp_ratio min sample_ratio
        
        val All_sample_cards = AllData.sample(false, Ratio, 0).select("pri_acct_no_conv").distinct()//.persist(StorageLevel.MEMORY_AND_DISK_SER) 
        
        All_sample_cards.rdd.map(r=>r.getString(0)).coalesce(1).saveAsTextFile(rangedir + "All_sample_cards")
    }
    
    def save_Alldata_bycards_3(ss: SparkSession): Unit ={
        val sc = ss.sparkContext
        val counterfeit_cards= sc.textFile(rangedir + "counterfeit_cards").collect 
        
        normal_cards_num = counterfeit_cards_num * TF_ratio
                
        val sample_cards= sc.textFile(rangedir + "All_sample_cards").takeSample(false, normal_cards_num.toInt, 0)  
          
        var all_cards_list = sample_cards.union(counterfeit_cards)
            
        var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000) 
       
        val Alldata_by_cards = AllData.filter(AllData("pri_acct_no_conv").isin(all_cards_list:_*))
         
        Alldata_by_cards.selectExpr(usedArr_filled:_*).rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "Alldata_by_cards")
    }
    
    def save_labeled_new_4(ss: SparkSession): Unit ={
        val sc = ss.sparkContext
        val counterfeit_cards= sc.textFile(rangedir + "counterfeit_cards").cache
        val counterfeit_cards_list = counterfeit_cards.collect()
        
        val sample_cards= sc.textFile(rangedir + "All_sample_cards").cache
        val all_cards = sample_cards.union(counterfeit_cards)
        val all_cards_list = all_cards.collect()
       
        var Alldata_by_cards_dir = rangedir + "Alldata_by_cards"
        var counterfeit_dir = rangedir + "counterfeit_filled"
        var Alldata_by_cards_filled = IntelUtil.get_from_HDFS.get_processed_DF(ss, Alldata_by_cards_dir)
        var counterfeit_filled = IntelUtil.get_from_HDFS.get_processed_DF(ss, counterfeit_dir)
        
 ///////////////////////////////////////////////////////过滤交易次数过多的账号////////////////////////////////////////////////       
        val countdf = Alldata_by_cards_filled.groupBy("pri_acct_no_conv_filled").agg(count("trans_at_filled") as "counts") 
        val filtered_cards = countdf.filter(countdf("counts")<1000)
        filtered_cards.show(5)
        println("filtered cards count is " + filtered_cards.count)
         
        Alldata_by_cards_filled = Alldata_by_cards_filled.join(filtered_cards, Alldata_by_cards_filled("pri_acct_no_conv_filled")===filtered_cards("pri_acct_no_conv_filled"), "leftsemi")
        println("Alldata_by_cards_filled count is " + Alldata_by_cards_filled.count) 
        Alldata_by_cards_filled.show(5)
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////    
      
        var normaldata_filled = Alldata_by_cards_filled.except(counterfeit_filled)
          
        var counterfeit_related_all_data = Alldata_by_cards_filled.filter(Alldata_by_cards_filled("pri_acct_no_conv_filled").isin(counterfeit_cards_list:_*))
        println("normaldata_filled count is " + normaldata_filled.count) 
        
        val udf_Map0 = udf[Double, String]{xstr => 0.0}
        val udf_Map1 = udf[Double, String]{xstr => 1.0}
         
        var NormalData_labeled = normaldata_filled.withColumn("isFraud", udf_Map0(normaldata_filled("trans_md_filled")))
        var counterfeit_labeled = counterfeit_filled.withColumn("isFraud", udf_Map1(counterfeit_filled("trans_md_filled")))
        var LabeledData = counterfeit_labeled.unionAll(NormalData_labeled)
        
        LabeledData.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "Labeled_All")
    }
  
    
}