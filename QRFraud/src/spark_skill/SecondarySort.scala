package spark_skill
//对比MovingAverage使用方法
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


//官方建议，如果需要在repartition重分区之后，还要进行排序，建议直接使用repartitionAndSortWithinPartitions算子。因为该算子可以一边进行重分区的shuffle操作，一边进行排序。shuffle与sort两个操作同时进行，比先shuffle再sort来说，性能可能是要高的。
//1.定义输入和输出为:  二次排序        数据少，所以val partitions = 1
//x,2,9
//y,2,5
//x,1,3
//y,1,7
//y,3,1
//x,3,6
//z,1,4
//z,2,8
//z,3,7
//z,4,0
//输出
//(z-4,0)
//(z-3,7)
//(z-2,8)
//(z-1,4)
//(y-3,1)
//(y-2,5)
//(y-1,7)
//(x-3,6)
//(x-2,9)
//(x-1,3)


//基本不用改
class CustomPartitioner(partitions: Int) extends Partitioner {
  require(partitions > 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  //这里定义分区规则  对输入的key做计算，然后返回该key的分区ID
  def getPartition(key: Any): Int = key match {
    case (k: String, v: Int) => math.abs(k.hashCode % numPartitions)
    case null                => 0
    case _                   => math.abs(key.hashCode % numPartitions)
  }

  //之所以要求用户实现这个函数是因为Spark内部会比较两个RDD的分区是否一样  
  override def equals(other: Any): Boolean = other match {
    case h: CustomPartitioner => h.numPartitions == numPartitions
    case _                    => false
  }

  //这里定义partitioner个数
  override def hashCode: Int = numPartitions
}


object SecondarySort {
  
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


    val partitions = 1
    val inputPath = "xrli/testSecondarySort.csv"
 
    val input = sc.textFile(inputPath)

   
    val valueToKey = input.map(x => {
      val line = x.split(",")
      ((line(0) + "-" + line(1), line(2).toInt), line(2).toInt)
    })    
    
    //隐式转换比较的规则    tupleOrderingDesc这个名字随便起
    implicit def tupleOrderingDesc = new Ordering[Tuple2[String, Int]] {
      override def compare(x: Tuple2[String, Int], y: Tuple2[String, Int]): Int = {
        if (y._1.compare(x._1) == 0)
          y._2.compare(x._2)
        else 
          y._1.compare(x._1)
      }
    }

    val sorted = valueToKey.repartitionAndSortWithinPartitions(new CustomPartitioner(partitions))    //repartitionAndSortWithinPartitions，但是该函数需要传入的数据类型要求为（key, value），

    val result = sorted.map {
      case (k, v) => (k._1, v)
    }

    result.collect().foreach(println)

    // done
    sc.stop()
  }
}
