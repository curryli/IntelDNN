package node2vec

import java.io.Serializable
import org.apache.spark.{SparkContext, SparkConf}
 
object Main {
  object Command extends Enumeration {
    type Command = Value
    val node2vec, randomwalk, embedding = Value
  }
  import Command._

  case class Params(iter: Int = 10,
                    lr: Double = 0.025,
                    numPartition: Int = 10,
                    dim: Int = 128,
                    window: Int = 10,
                    walkLength: Int = 80,
                    numWalks: Int = 10, //每个点作为起始点的次数
                    p: Double = 1.0,
                    q: Double = 1.0,
                    weighted: Boolean = true,
                    directed: Boolean = false,
                    degree: Int = 30,
                    indexed: Boolean = false,
                    nodePath: String = "xrli/node2id.csv",
                    input: String = "xrli/edgefile.csv",
                    output: String = "xrli/emd_out"
                    )
  val defaultParams = new Params()
   
  
  def main(args: Array[String]) = {
    
      val conf = new SparkConf().setAppName("Node2Vec")
      val context: SparkContext = new SparkContext(conf)
      
      Node2vec.setup(context, defaultParams)
   
      
      Node2vec.load().initTransitionProb().randomWalk().embedding().save()
     
       //只保存节点序列可以用下列语句
      //Node2vec.load().initTransitionProb().randomWalk().saveRandomPath()
     
  }
}





//输入格式
//node1_id_int 	node2_id_int 	<weight_float, optional>
//or
//node1_str 	node2_str 	<weight_float, optional>, Please set the option "indexed" to false


//If "indexed" is set to false, node2vec_spark index nodes in input edgelist, example: 
//unindexed edgelist:
//node1 node2 1.0
//node2 node7 1.0
//
//indexed:
//1 2 1.0
//2 3 1.0
//需要指定  nodePath
//1 node1
//2 node2
//3 node7
