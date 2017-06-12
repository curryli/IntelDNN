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


object SaveFraud_HDFS {
  

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);

    //    require(args.length == 3)

    val conf = new SparkConf().setAppName("SaveFraud_HDFS")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
 
    val startTime = System.currentTimeMillis(); 
    
    //注意select * 会出现内存溢出报错，估计是1行太多了，java堆栈不够，起始可以设置  http://blog.csdn.net/oaimm/article/details/25298691  但这里就少选几列就可以了
    var Fraud_join = hc.sql(s"select sys_tra_no,ar_pri_acct_no,mchnt_cd,trans_dt,fraud_tp "+
      s"from tbl_arsvc_fraud_trans "+ 
      s"where trans_dt>=20160101 and trans_dt<=20161231 ")//.repartition(1000).persist(StorageLevel.MEMORY_AND_DISK_SER)  //.cache 
        
    
      println("Fraud_join.count(): " + Fraud_join.count())
      Fraud_join.rdd.map { x => x.toSeq.mkString("\t") }.saveAsTextFile("xrli/IntelDNN/Fraud_join_2016")
 
  }
  
  
  
  
}