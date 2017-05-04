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
import Algorithm._
import scala.collection.mutable.MutableList
import scala.Range
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import Algorithm._
import org.apache.spark.sql.functions._

object compare_2time {
  

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);

    //    require(args.length == 3)

    val conf = new SparkConf().setAppName("compare_2time")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
   
    
    var transdata = hc.sql(s"select trans_at,orig_trans_at " +
        s"from tbl_common_his_trans where pdate=20160701").cache
        
    println("transdata count: " + transdata.count())
 
    val cols2 = transdata.map(f =>(f.getString(0).toLong, f.getString(1).toLong))
    
    println("Equal count: " + cols2.filter(f => f._1 == f._2).count())
    println("More count: " + cols2.filter(f => f._1 > f._2).count())
    println("Less count: " + cols2.filter(f => f._1 < f._2).count())
    println("orig_trans_at 0 count: " + cols2.filter(f => f._2 == 0).count())
    
    println("Equal count: " + transdata.filter("trans_at = orig_trans_at").count())
    println("More count: " + transdata.filter("trans_at > orig_trans_at").count())
    println("Less count: " + transdata.filter("trans_at < orig_trans_at").count())
    println("orig_trans_at 0 count: " + transdata.filter("orig_trans_at=0").count())
    
    
 
  }
  
  
  
  
  
  
}