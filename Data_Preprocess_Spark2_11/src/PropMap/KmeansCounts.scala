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
import org.apache.spark.mllib.linalg.{Vector, Vectors}

 
object KmeansCounts {
  case class centerIndex(id: Int, center: org.apache.spark.ml.linalg.Vector)
  
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);
    
    //val sparkConf = new SparkConf().setAppName("spark2SQL")
    val warehouseLocation = "spark-warehouse"
    
    val ss = SparkSession
      .builder()
      .appName("Spark CountTimes")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.schema.verification", false)
      .getOrCreate()
  
    import ss.implicits._
    val sc = ss.sparkContext
 
    val startTime = System.currentTimeMillis(); 
     
    val startdate = "20160702"
    val enddate = "20160702"
    var AllData = IntelUtil.get_from_HDFS.get_origin_DF(ss, startdate, enddate)//.sample(false, 0.0001, 0) 
    
    println("AllData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
 
    AllData.na.fill("NULL")
    AllData.show()
     
	  var countdf = AllData.groupBy("pri_acct_no_conv").agg(count("trans_at") as "counts").select("counts")
     
    var indexer = new StringIndexer().setInputCol("counts").setOutputCol("counts_CatVec").setHandleInvalid("skip")
    countdf = indexer.fit(countdf).transform(countdf)
    println("indexer done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    

        
    val assembler = new VectorAssembler()
      .setInputCols(Array("counts_CatVec"))
      .setOutputCol("features_Vec")
     
    countdf = assembler.transform(countdf)
  
    val kn = 10
    val kmeans = new KMeans().setK(kn).setSeed(1L).setFeaturesCol("features_Vec").setPredictionCol("prediction")
    
    val kmodel = kmeans.fit(countdf)
    
//    for (center <-kmodel.clusterCenters) {
//      println(" "+ center)
//    }
//    for(i<- 0 to 4){
//      var dfresult = KmeansResult.filter(KmeansResult("prediction")===i)
//      println("count==" + i + " num is: "+ dfresult.count())
//    }
    
    
    var centerIndexlist = List[centerIndex]()
    val centerlist = kmodel.clusterCenters
    for(i <- 0 to kn-1){
       centerIndexlist = centerIndexlist.::(centerIndex(i, centerlist(i)))
    }
    val centerIndexDF = ss.createDataFrame(centerIndexlist)//.withColumnRenamed("id", "center")
    centerIndexDF.show
    
    val KmeansResult = kmodel.transform(countdf)
    println("Kmeans done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
    KmeansResult.show() 
     

    
    val distributeDF = KmeansResult.groupBy("prediction").agg(count("counts_CatVec"))
    distributeDF.show
    
    var joinedDF = distributeDF.join(centerIndexDF, distributeDF("prediction") === centerIndexDF("id"), "left_outer") 
    joinedDF.show
  }
  
   
}