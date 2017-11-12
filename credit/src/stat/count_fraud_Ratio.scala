package stat
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


object count_fraud_Ratio {
    val startdate = "20160901"
    val enddate = "20160905"
    val rangedir = IntelUtil.varUtil.rangeDir 
    val usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
    
    //可以调整
    val sample_cards_ratio = 0.0005
    val TF_ratio = 500
    val fraudType = "04"
    
    
    var fraudType_cards_num = 0L
    var normal_cards_num = 0L
    var fraudType_related_fraud_count = 0L
     
 
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
    
    count_Ratio(ss)
   
    
     
  }
    
    
    def count_Ratio(ss: SparkSession): Unit ={
        var fraud_join_Data = IntelUtil.get_from_HDFS.get_fraud_join_DF(ss, startdate, enddate).persist(StorageLevel.MEMORY_AND_DISK_SER)
        var fraudType_infraud = fraud_join_Data.filter(fraud_join_Data("fraud_tp")=== fraudType) 
        var cnt = fraudType_infraud.count()
        println("weika count in week: ", fraudType_infraud.count())
//        var fraud_ratio = 80000000/(cnt/7)
//        println("approximate weika fraud_ratio: ", fraud_ratio)
        
        var AllData = IntelUtil.get_from_HDFS.get_origin_DF(ss, startdate, enddate).repartition(1000)
        var all_cnt = AllData.count
        println("all_cnt: ", all_cnt)   //192623
        
        
        val weika_chnl_list = Array("05","07","08","11","14","17","20","23","01","03")
        val Data_same_chnl  = AllData.filter(AllData("trans_chnl").isin(weika_chnl_list:_*))
        val Data_same_chnl_cnt = Data_same_chnl.count
        println("Data_same_chnl count: ", Data_same_chnl_cnt)   
        
        var real_ratio = Data_same_chnl_cnt/cnt
        println("Real weika fraud_ratio: ", real_ratio)   //9月1~5号  
        
          
         
    }
    
    //统计伪卡欺诈交易渠道 
    //select distinct(trans_chnl) from tbl_arsvc_fraud_trans where fraud_tp='04' and trans_dt>='20160901' and trans_dt<='20161101';
    //"05","07","08","11","14","17","20","23","01","03"
    
    
 // select card_attr,  count(*) from tbl_arsvc_fraud_trans where fraud_tp='04' and trans_dt>='20160901' and trans_dt<='20160930' group by card_attr;   
//伪卡 卡属性    
//01      1114
//02      3262
//03      263
    
  //select card_attr, count(*) from tbl_common_his_trans where pdate>='20160901' and pdate<='20160901'  group by card_attr;  
    
//        58629
//01      62525802
//02      26970942
//03      1255934
//05      558

}