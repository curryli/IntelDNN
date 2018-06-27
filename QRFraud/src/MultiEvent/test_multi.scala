
package MultiEvent
//这个好像可以成功
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
 
import SparkContext._ 
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

object test_multi { 
   class VertexProperty()// extends Serializable  
   class EdgePropery()// extends Serializable 
   
   
   case class prop_VP[U: ClassTag](
     val prop: U
     ) extends VertexProperty
     
   
   case class prop_EP[U: ClassTag](
     val prop: U
     ) extends EdgePropery
 
     

  def main(args: Array[String]) { 
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.INFO)
    Logger.getLogger("org").setLevel(Level.INFO);
    Logger.getLogger("akka").setLevel(Level.INFO);
    Logger.getLogger("hive").setLevel(Level.INFO);
    Logger.getLogger("parse").setLevel(Level.INFO);
    
    val sparkConf = new SparkConf().setAppName("MultiCompute")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)  
  
    val startTime = System.currentTimeMillis(); 
    
    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String,Int)
    val vertexArray = Array(
      (1L, ("card", "a_Card")),
      (2L, ("card", "b_Card")),
      (3L, ("card", "c_Card")),
      (4L, ("POS", "d_POS_d")),
      (5L, ("POS", "e_POS_e"))
    )
    
    
    //边的数据类型ED
    val edgeArray = Array(
      Edge(1L, 2L, "Transfer"),
      Edge(1L, 3L, "Transfer"),
      Edge(1L, 5L, "Spend"),
      Edge(1L, 4L, "Withdraw"),
      Edge(3L, 4L, "Withdraw")
    )
    
    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, String))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[String]] = sc.parallelize(edgeArray)
    
    //构造图Graph[VD,ED]
    val graph: Graph[(String, String), String] = Graph(vertexRDD, edgeRDD)
    
    println("找出图中card顶点：")
    graph.vertices.filter { case (id, (vtype, name)) => vtype == "card"}.collect.foreach {
      case (id, (vtype, name)) => println(s"$name is $vtype")
    }
    
     //边操作：找出图中属性"Transfer"边
    println("找出图中属性Transfer的边：")
    graph.edges.filter(e => e.attr == "Transfer").collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
  
    
    
    sc.stop()
    
  } 
  
} 
 
 