package spark_skill

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//输入  股票价格(k,t,v)，        输出在其特定的时间内输出(k,t,周期内平均值)
 
  
object MovingAverage2 {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MovingAverage")
    val sc = new SparkContext(sparkConf)

    val window = 5
    val numPartitions = 1
    val input = "xrli/testMovingAverage.csv"

    val brodcastWindow = sc.broadcast(window)

    val rawData = sc.textFile(input)
    val keyValue = rawData.map(line => {
      val tokens = line.split(",")
      (tokens(0), (tokens(1), tokens(2).toDouble))
    })

    // Key being stock symbol like IBM, GOOG, AAPL, etc
    val groupByStockSymbol = keyValue.combineByKey(
      (v : (String, Double)) => List(v),                 //--将1 转换成 list(1)
      (c : List[(String, Double)], v : (String, Double)) => v :: c,            //--将list(1)和2进行组合从而转换成list(1,2)
      (c1 : List[(String, Double)], c2 : List[(String, Double)]) => c1 ::: c2          //--将全局相同的key的value进行组合
    )
    

    val result = groupByStockSymbol.mapValues(values => {
      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")      
      // in-memory sorting, will not scale with large datasets  // 在内存中排序，mapValues只对value操作，其中s._1为时间，s._2为价格
      val sortedValues = values.map(s => (dateFormat.parse(s._1).getTime.toLong, s._2)).toSeq.sortBy(_._1) 
      val queue = new scala.collection.mutable.Queue[Double]()
      val new_value = for (tup <- sortedValues) yield {
        queue.enqueue(tup._2)
        if (queue.size > brodcastWindow.value)
          queue.dequeue
        //不管在for里面还是yield里面都可以进行额外的操作，最后一句话是每次返回值
        (dateFormat.format(new java.util.Date(tup._1)), (queue.sum / queue.size))
      }
      new_value
     }
    )

    // output will be in CSV format
    // <stock_symbol><,><date><,><moving_average>
    val formattedResult = result.flatMap(kv => {
      kv._2.map(v => (kv._1 + "," + v._1 + "," + v._2.toString()))
    })
 
    
    formattedResult.collect().foreach(println)
    
    //done 
    sc.stop()
  }

}


