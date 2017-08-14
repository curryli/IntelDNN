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


object SaveLabelFlow {
    val startdate = IntelUtil.varUtil.startdate
    val enddate = IntelUtil.varUtil.enddate
    val rangedir = IntelUtil.varUtil.rangeDir 
    val usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
    
    //可以调整
    val sample_cards_ratio = 0.0001
    val TF_ratio = 500
    val fraudType = "62"
    
    
    var fraudType_cards_num = 0L
    var normal_cards_num = 0L
    var fraudType_fraud_count = 0L
     
 
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
    
    save_fraudType_1(ss)
    println("fraudType_cards count is " + fraudType_cards_num)
    println("step1 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
      
    save_normal_cards_2(ss)
    println("step2 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
    save_Alldata_bycards_3(ss)
    println("step3 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
    save_labeled_new_4(ss)
    println("step4 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
  }
    
    
    def save_fraudType_1(ss: SparkSession): Unit ={
        var fraud_join_Data = IntelUtil.get_from_HDFS.get_fraud_join_DF(ss, startdate, enddate).persist(StorageLevel.MEMORY_AND_DISK_SER)
         
        var fraudType_infraud = fraud_join_Data.filter(fraud_join_Data("fraud_tp")=== fraudType) 
        
        var fraudType_cards = fraudType_infraud.select("pri_acct_no_conv").distinct().persist(StorageLevel.MEMORY_AND_DISK_SER) 
        fraudType_cards_num = fraudType_cards.count()
        //AllFlow.fraudType_cards_num = fraudType_cards.count()
     
        fraudType_cards.rdd.map(r=>r.getString(0)).saveAsTextFile(rangedir + "fraudType_cards")
     
    	  fraud_join_Data.unpersist(false)
    	  
    	  var fraudType_cards_list = fraudType_cards.rdd.map(r=>r.getString(0)).collect()
    	
        var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000) 
         
        val fraudType_related = AllData.filter(AllData("pri_acct_no_conv").isin(fraudType_cards_list:_*))
        println("fraudType_related_all_data count is " + fraudType_related.count()) 
         
        var fraudType_fraud = fraudType_related.join(fraudType_infraud, fraudType_related("sys_tra_no")===fraudType_infraud("sys_tra_no"), "leftsemi")
        
        var fraudType_unsure = fraudType_related.join(fraudType_infraud, fraudType_related("sys_tra_no").!==(fraudType_infraud("sys_tra_no")), "leftsemi")

        
        
        val fraudType_filled = fraudType_fraud.selectExpr(usedArr_filled:_*)
        fraudType_fraud_count = fraudType_filled.count()
        println("fraudType_fraud count is " + fraudType_fraud_count)  
        fraudType_filled.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "fraudType_filled")
        
        
        val fraud_unsure_filled = fraudType_unsure.selectExpr(usedArr_filled:_*)
        println("fraud_unsure count is " + fraud_unsure_filled.count)  
        fraud_unsure_filled.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "fraud_unsure_filled")
         
    }
    
    
    def save_normal_cards_2(ss: SparkSession, sample_ratio: Double = sample_cards_ratio): Unit ={
        val sc = ss.sparkContext
        val fraudType_cards= sc.textFile(rangedir + "fraudType_cards").collect   
      
        var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000) 
        var tmp_ratio = fraudType_fraud_count.toDouble/80000000.toDouble
        println("tmp_ratio count: ", tmp_ratio, "sample_ratio count: ", sample_ratio)
        
        var Ratio =  tmp_ratio min sample_ratio
        
        val All_sample_cards = AllData.sample(false, Ratio, 0).select("pri_acct_no_conv").distinct()//.persist(StorageLevel.MEMORY_AND_DISK_SER) 
        println("All_sample_cards count is " + All_sample_cards.count)  
        
        val Norm_sample_cards = All_sample_cards.filter(!All_sample_cards("pri_acct_no_conv").isin(fraudType_cards:_*))
        println("Norm_sample_cards count is " + Norm_sample_cards.count)  
               
        Norm_sample_cards.rdd.map(r=>r.getString(0)).coalesce(1).saveAsTextFile(rangedir + "Norm_sample_cards")
    }
    
    def save_Alldata_bycards_3(ss: SparkSession): Unit ={
        val sc = ss.sparkContext
        val fraudType_cards= sc.textFile(rangedir + "fraudType_cards").collect 
        
        normal_cards_num = fraudType_cards_num * TF_ratio
                
        val sample_cards= sc.textFile(rangedir + "Norm_sample_cards").takeSample(false, normal_cards_num.toInt, 0)  
          
        var all_cards_list = sample_cards               //.union(fraudType_cards)  //套现要删掉
            
        var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000) 
       
        val Normdata_by_cards = AllData.filter(AllData("pri_acct_no_conv").isin(all_cards_list:_*))
  
        Normdata_by_cards.selectExpr(usedArr_filled:_*).rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "Normdata_by_cards")
    }
    
    def save_labeled_new_4(ss: SparkSession): Unit ={
        val sc = ss.sparkContext
        val fraudType_cards= sc.textFile(rangedir + "fraudType_cards").cache
        val fraudType_cards_list = fraudType_cards.collect()
        
        val Norm_cards= sc.textFile(rangedir + "Norm_sample_cards").cache
       
        var Normdata_dir = rangedir + "Normdata_by_cards"
        var fraudType_dir = rangedir + "fraudType_filled"
        var fraud_unsure_dir = rangedir + "fraud_unsure_filled"
        
        
        var Normdata_filled = IntelUtil.get_from_HDFS.get_processed_DF(ss, Normdata_dir)
        var fraudType_filled = IntelUtil.get_from_HDFS.get_processed_DF(ss, fraudType_dir)
        var fraud_unsure_filled = IntelUtil.get_from_HDFS.get_processed_DF(ss, fraud_unsure_dir)
        

       
        val udf_Map0 = udf[Double, String]{xstr => 0.0}
        val udf_Map1 = udf[Double, String]{xstr => 1.0}
        val udf_Map2 = udf[Double, String]{xstr => 2.0}
         
        var NormalData_labeled = Normdata_filled.withColumn("isFraud", udf_Map0(Normdata_filled("trans_md_filled")))
        var fraudType_labeled = fraudType_filled.withColumn("isFraud", udf_Map1(fraudType_filled("trans_md_filled")))
        var fraud_unsure_labeled = fraud_unsure_filled.withColumn("isFraud", udf_Map2(fraud_unsure_filled("trans_md_filled")))
 
        
        var LabeledData = fraudType_labeled.unionAll(NormalData_labeled).unionAll(fraud_unsure_labeled)
        
        
///////////////////////////////////////////////////////过滤交易次数过多的账号////////////////////////////////////////////////       
        val countdf = LabeledData.groupBy("pri_acct_no_conv_filled").agg(count("trans_at_filled") as "counts") 
        val filtered_cards = countdf.filter(countdf("counts")<1000)
        filtered_cards.show(5)
        println("filtered cards count is " + filtered_cards.count)
         
        LabeledData = LabeledData.join(filtered_cards, LabeledData("pri_acct_no_conv_filled")===filtered_cards("pri_acct_no_conv_filled"), "leftsemi")
        println("filtered LabeledData count is " + LabeledData.count) 
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////    
        
        
        LabeledData.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "Labeled_All")
    }
  
    
}