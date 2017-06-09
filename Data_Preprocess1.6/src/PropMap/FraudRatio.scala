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

object FraudRatio {
  

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);

    //    require(args.length == 3)

    val conf = new SparkConf().setAppName("Graphx Test")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
   
    //var sumList = List()
    
//    for(i <- 0 to 30){
//      var date = (20160701 + i).toString()
//      //DateList = DateList.::(date)
//      var transdata = hc.sql(s"select sum(trans_at) from tbl_common_his_trans where pdate=$date")
//      val money = transdata.take(1)(0).getDouble(0)
//      println(money)
//    }
    
      for(i <- 0 to 30){
      var date = (20160701 + i).toString()
      //DateList = DateList.::(date)
      var transdata = hc.sql(s"select sum(trans_at) from tbl_arsvc_fraud_trans where trans_dt=$date")
      val money = transdata.take(1)(0).getDouble(0)
      println(money)
    }
 
  }
  
  
  
  
  
  
}