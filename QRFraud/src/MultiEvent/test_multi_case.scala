
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

object test_multi_case { 
   class VertexProperty() extends Serializable  
   class EdgePropery() extends Serializable 
   
   
   case class prop_VP[U: ClassTag](
     val prop: U
     ) extends VertexProperty
     
   
   case class prop_EP[U: ClassTag](
     val prop: U
     ) extends EdgePropery
 
   
  case class Edge_Property_Class(
     val edgeType: String,
     val amount: Double
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
      (1L, new prop_VP("card", "a_Card")),
      (2L, new prop_VP("card", "b_Card")),
      (3L, new prop_VP("card", "c_Card", 666)),
      (4L, new prop_VP("POS", "d_POS_d", 333)),
      (5L, new prop_VP("POS", "d_POS_d"))
    )
    
    
    //边的数据类型ED
    val edgeArray = Array(
      Edge(1L, 2L, new prop_EP("Transfer")),
      Edge(1L, 3L, new prop_EP("Transfer")),
      Edge(1L, 5L, new prop_EP("Spend")),
      Edge(1L, 4L, new prop_EP("Withdraw")),
      Edge(3L, 4L, new prop_EP("Withdraw"))
    )
    
    //构造vertexRDD和edgeRDD
    val vertexRDD = sc.parallelize(vertexArray)
    val edgeRDD = sc.parallelize(edgeArray)
    
    //构造图Graph[VD,ED]
    val graph = Graph(vertexRDD, edgeRDD)
    
    println("find card nodes")
    graph.vertices.filter { f => f._2.prop.productIterator.contains("card")}.collect.foreach {f=>
       println(f._1 + " is " + f._2)
    }
    
     graph.vertices.filter { f => f._2.prop.productIterator.toList(0)=="card"}.collect.foreach {f=>
       println(f._1 + " is " + f._2)
    }
     
     
    
     //边操作：找出图中属性"Transfer"边
    println("find Transfer edges")
    graph.edges.filter(e => e.attr == "Transfer").collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
   
    sc.stop()
    
  } 
  
} 
 
 