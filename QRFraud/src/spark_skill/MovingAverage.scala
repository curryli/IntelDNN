package spark_skill

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//输入  股票价格(k,t,v)，        输出在其特定的时间内输出(k,t,周期内平均值)

//输入
//GOOG,2004-11-04,184.70
//GOOG,2004-11-03,184.70
//GOOG,2004-11-02,184.70
//AAPL,2013-10-09,486.20
//AAPL,2013-10-08,483.77
//AAPL,2013-10-07,480.32
//AAPL,2013-10-04,486.75
//AAPL,2013-10-03,482.14
//IBM,2013-11-08,183.77
//IBM,2013-11-07,180.32
//IBM,2013-11-04,186.75
//GOOG,2013-11-04,884.70
//GOOG,2013-11-03,884.70
//GOOG,2013-11-02,884.70

//输出
//IBM,2013-11-04,186.75
//IBM,2013-11-07,183.535
//IBM,2013-11-08,183.61333333333334
//GOOG,2004-11-02,184.7
//GOOG,2004-11-03,184.7
//GOOG,2004-11-04,184.69999999999996
//GOOG,2013-11-02,359.7
//GOOG,2013-11-03,464.7
//GOOG,2013-11-04,604.7
//AAPL,2013-10-03,482.14
//AAPL,2013-10-04,484.445
//AAPL,2013-10-07,483.07
//AAPL,2013-10-08,483.245
//AAPL,2013-10-09,483.83599999999996


// Case class comes handy
case class CompositeKey(stockSymbol: String, timeStamp: Long)  //k,t
case class TimeSeriesData(timeStamp: Long, closingStockPrice: Double)  //t,v

// Defines ordering
object CompositeKey {
  implicit def my_ordering[A <: CompositeKey]: Ordering[A] = {
    Ordering.by(fk => (fk.stockSymbol, fk.timeStamp))
  }
}


//---------------------------------------------------------
// the following class defines a custom partitioner by
// extending abstract class org.apache.spark.Partitioner
//---------------------------------------------------------
import org.apache.spark.Partitioner

class CompositeKeyPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case k: CompositeKey => math.abs(k.stockSymbol.hashCode % numPartitions)
    case null            => 0
    case _               => math.abs(key.hashCode % numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: CompositeKeyPartitioner => h.numPartitions == numPartitions
    case _                          => false
  }

  override def hashCode: Int = numPartitions
}


object MovingAverage {
  
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
      (CompositeKey(tokens(0), timestamp), TimeSeriesData(timestamp, tokens(2).toDouble))
    })

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


