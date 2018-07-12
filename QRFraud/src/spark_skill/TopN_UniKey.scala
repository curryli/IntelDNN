package spark_skill

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Partitioner
import scala.collection.SortedMap

 
object TopN_UniKey {
  
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);

    //    require(args.length == 3)

    val conf = new SparkConf().setAppName("QR_fraud")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)


    val N = sc.broadcast(10)
    val path = args(0)

    val input = sc.textFile(path)
    
//1,cat1,1
//2,cat2,2
//3,cat3,3
//4,cat4,4
    
    val pair = input.map(line => {
      val tokens = line.split(",")
      (tokens(2).toInt, tokens)
    })

    import Ordering.Implicits._
    val partitions = pair.mapPartitions(itr => {
      var sortedMap = SortedMap.empty[Int, Array[String]]   //按照键的排序访问
      itr.foreach { tuple =>
        {
          sortedMap += tuple
          if (sortedMap.size > N.value) {
            sortedMap = sortedMap.takeRight(N.value)
          }
        }
      }
      sortedMap.takeRight(N.value).toIterator
    })

    val alltop10 = partitions.collect()
    val finaltop10 = SortedMap.empty[Int, Array[String]].++:(alltop10)
    val resultUsingMapPartition = finaltop10.takeRight(N.value)
    
    //Prints result (top 10) on the console
    resultUsingMapPartition.foreach {
      case (k, v) => println(s"$k \t ${v.asInstanceOf[Array[String]].mkString(",")}")
    }
    
    
    
    
    

    // Below is additional approach which is more concise
    val moreConciseApproach = pair.groupByKey().sortByKey(false).take(N.value)

    //Prints result (top 10) on the console
    moreConciseApproach.foreach {
      case (k, v) => println(s"$k \t ${v.flatten.mkString(",")}")
    }
    
    // done
    sc.stop()
  }
}