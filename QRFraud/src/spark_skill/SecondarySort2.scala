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
 
//更容易看懂
class SecordSortKey(val firstKey: String, val secondKey: Int)extends Ordered[SecordSortKey] with Serializable{
    override def compare(that: SecordSortKey):Int = {
      if(this.firstKey != that.firstKey) {
        this.firstKey.compare(that.firstKey)
      }else {
        this.secondKey.compare(that.secondKey)
      }
    }  
  }

object SecondarySort2 {
  
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

 
    val inputPath = "xrli/testSecondarySort.csv"
 
    val input = sc.textFile(inputPath)

    //第二步：将要进行二次排序的数据加载，按照<key，value>格式的RDD
    val valueToKey = input.map(x => {
      val line = x.split(",")
      (new SecordSortKey(line(0) + "-" + line(1), line(2).toInt), x)
    })    
    
    //第三步：使用sortByKey 基于自定义的key进行二次排序

    val sorted = valueToKey.sortByKey(false);

    val result = sorted.map(f=>f._2)

    result.collect().foreach(println)

    // done
    sc.stop()
  }
}
