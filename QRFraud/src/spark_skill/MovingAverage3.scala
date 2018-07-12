package spark_skill

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//输入  股票价格(k,t,v)，        输出在其特定的时间内输出(k,t,周期内平均值)
 
 // Case class comes handy
case class CompositeKey2(stockSymbol: String, timeStamp: Long)  //k,t
case class TimeSeriesData2(timeStamp: Long, closingStockPrice: Double)  //t,v

object MovingAverage3 {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MovingAverage")
    val sc = new SparkContext(sparkConf)

    val window = 5
    val numPartitions = 1
    val input = "xrli/testMovingAverage.csv"

    val brodcastWindow = sc.broadcast(window)

    val rawData = sc.textFile(input)

    // Key contains part of value (closing date in this case)
    val valueTokey = rawData.map(line => {
      val tokens = line.split(",")
      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val timestamp = dateFormat.parse(tokens(1)).getTime
      (CompositeKey2(tokens(0), timestamp), TimeSeriesData2(timestamp, tokens(2).toDouble))
    })

//    implicit def my_ordering[A <: CompositeKey2]: Ordering[A] = {
//    Ordering.by(fk => (fk.stockSymbol, fk.timeStamp))
//  }
     
    implicit def my_ordering = new Ordering[CompositeKey2] {
      override def compare(x: CompositeKey2, y: CompositeKey2): Int = {
        if (y.stockSymbol != x.stockSymbol)
           y.stockSymbol.compare(x.stockSymbol)
        else 
           y.timeStamp.compare(x.timeStamp)
      }
    }
     
    // 进行分区再分组的二次排序，其中CompositeKeyPartitioner进行分区，CompositeKey进行内排序
    val sortedData = valueTokey.repartitionAndSortWithinPartitions(new CompositeKeyPartitioner(numPartitions))
    
    val keyValue = sortedData.map(k => (k._1.stockSymbol, (k._2)))
    val groupByStockSymbol = keyValue.groupByKey()   //combineByKey
    
    val movingAverage = groupByStockSymbol.mapValues(values => {
      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val queue = new scala.collection.mutable.Queue[Double]()
      for (TimeSeriesData <- values) yield {
        queue.enqueue(TimeSeriesData.closingStockPrice)
        if (queue.size > brodcastWindow.value)
          queue.dequeue

        (dateFormat.format(new java.util.Date(TimeSeriesData.timeStamp)), (queue.sum / queue.size))
      }
    })
    
    // output will be in CSV format
    // <stock_symbol><,><date><,><moving_average>
    val formattedResult = movingAverage.flatMap(kv => {
      kv._2.map(v => (kv._1 + "," + v._1 + "," + v._2.toString()))
    })
    
    
    formattedResult.collect().foreach(println)
    // done
    sc.stop()
  }

}


