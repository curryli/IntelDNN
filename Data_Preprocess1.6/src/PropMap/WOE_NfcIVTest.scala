package PropMap

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.UserDefinedFunction


object NfcIVTest {

  val tableName = "arlabdb.tbl_arlab_nfc_final_v1_flag6_dis"
  val  woeTable=tableName+"_woe"
  val flagColumn = "flag1"
  val normalFlag = "0"
  val fraudFlag = "1"
  val beginDate="20160601"
  val endDate="20170301"

  /**
   * 1. 计算WOE值
   */
  def getWOEByColumn(hc: HiveContext, ivColumn: String, normalCount: Long, fraudCount: Long): DataFrame = {
    hc.sql(s"select  $ivColumn,ln(p1/p0) as woe from (" +
      s"select $ivColumn,normalCount/$normalCount as p0,fraudCount/$fraudCount as p1 from (" +
      s"select $ivColumn, if(normalCount=0,1,normalCount) as normalCount,  if(fraudCount=0,1,fraudCount) as fraudCount from (" +
      s"select $ivColumn, sum(if($flagColumn='$normalFlag',1,0)) as normalCount, sum(if($flagColumn='$fraudFlag',1,0)) as fraudCount " +
      s"from $tableName " +
      s"where  unix_timestamp(trans_dt_tm,'yyyyMMddHHmmss') >= unix_timestamp('$beginDate','yyyyMMdd') " +
      s"and unix_timestamp(trans_dt_tm,'yyyyMMddHHmmss') < unix_timestamp('$endDate','yyyyMMdd') " +
//      s"and goods_tp not  in ('1004','1005') " +
      s" group by $ivColumn )a)b )c")

  }

  /**
   * 2. 计算IV值
   */
  def getIVByColumn(hc: HiveContext,tableName:String, ivColumn: String, normalCount: Long, fraudCount: Long): Double = {
    val ivTest = hc.sql(s"select sum((p1-p0)*ln(p1/p0)) as iv from (" +
      s"select $ivColumn,normalCount/$normalCount as p0,fraudCount/$fraudCount as p1 from (" +
      s"select $ivColumn, if(normalCount=0,1,normalCount) as normalCount,  if(fraudCount=0,1,fraudCount) as fraudCount from (" +
      s"select $ivColumn, sum(if($flagColumn='$normalFlag',1,0)) as normalCount, sum(if($flagColumn='$fraudFlag',1,0)) as fraudCount " +
      s"from $tableName " +
      s"where  unix_timestamp(trans_dt_tm,'yyyyMMddHHmmss') >= unix_timestamp('$beginDate','yyyyMMdd') " +
      s"and unix_timestamp(trans_dt_tm,'yyyyMMddHHmmss') < unix_timestamp('$endDate','yyyyMMdd') " +
//      s"and goods_tp not  in ('1004','1005') " +
      s" group by $ivColumn )a)b )c")
    ivTest.first().getDouble(0)
  }

  /**
   * 3. 计算离散变量取值个数
   */
  def getValueCount(hc: HiveContext,tableName:String, ivColumn: String): Long = {
    hc.sql(s"select $ivColumn from $tableName " +
      s"where  unix_timestamp(trans_dt_tm,'yyyyMMddHHmmss') >= unix_timestamp('$beginDate','yyyyMMdd') " +
      s"and unix_timestamp(trans_dt_tm,'yyyyMMddHHmmss') < unix_timestamp('$endDate','yyyyMMdd') " +
//      s"and goods_tp not   in ('1004','1005') " +
      s"group by $ivColumn ").count()
  }

  def main(args: Array[String]): Unit = {

//    Logger.getLogger("org").setLevel(Level.WARN);
//    Logger.getLogger("akka").setLevel(Level.WARN);
//    Logger.getLogger("hive").setLevel(Level.WARN);
//    Logger.getLogger("parse").setLevel(Level.WARN);
//
//    //    require(args.length == 3)
//
//    val conf = new SparkConf().setAppName("IVTest")
//    val sc = new SparkContext(conf)
//    val hc = new HiveContext(sc)
//    val sqlContext = new SQLContext(sc)
//
//    var data = hc.sql(s"select * from $tableName where " +
////      s" goods_tp not  in ('1004','1005') and   " +
//      s"unix_timestamp(trans_dt_tm,'yyyyMMddHHmmss') >=unix_timestamp('$beginDate','yyyyMMdd')  and " +
//      s"unix_timestamp(trans_dt_tm,'yyyyMMddHHmmss') < unix_timestamp('$endDate','yyyyMMdd') " )
//   
//     
//
//
//    val normalCount = data.filter(s"$flagColumn=$normalFlag").count()
//    val fraudCount = data.filter(s"$flagColumn=$fraudFlag").count()
//
//    data.filter(s"$flagColumn=$normalFlag").first().get(0)
//    
//
//    // 离散变量woe值转换
//    NfcFeature.categoryColumns.map { x =>
//      {
//        println("compute woe value for column " + x)
//
//        val woeDF = getWOEByColumn(hc, x, normalCount, fraudCount).cache()
//
//        var woeMap = collection.mutable.Map[Any, Double]()
//        woeDF.collect().foreach { f => { woeMap+=(f.get(0) -> f.getDouble(1)) } }
//        woeDF.collect().foreach { println}
//
////        println(woeMap.toString())
//
//        val woeCoder: (Any => Double) = (key: Any) => { woeMap.get(key).get }
//        val woeUDF = udf(woeCoder)
//        data = data.withColumn("woe_" + x, woeUDF(col(x)))
//      }
//
//    }
//    
//    //连续型变量woe值转换
//    
//        NfcFeature.numericColumns.map { x =>
//      {
//        println("compute woe value for column " + x)
//
//        val woeDF = getWOEByColumn(hc, "c_"+x, normalCount, fraudCount).cache()
//
//        var woeMap = collection.mutable.Map[Any, Double]()
//        woeDF.collect().foreach { f => { woeMap+=(f.get(0) -> f.getDouble(1)) } }
//        woeDF.collect().foreach { println}
//
//
////        println(woeMap.toString())
//
//        val woeCoder: (Any => Double) = (key: Any) => { woeMap.get(key).get }
//        val woeUDF = udf(woeCoder)
//        data = data.withColumn("woe_" + x, woeUDF(col("c_"+x)))
//      }
//
//    }
//        
//        
//    println("convert table by woe value ")
//    
//  
//    data.write.saveAsTable(woeTable)
//    
//   
//
//    NfcFeature.categoryColumns.map { x => 
//      val item="woe_"+x
//      println(x + " count:\t " + getValueCount(hc,woeTable, item) + "\tIV:\t" + getIVByColumn(hc,woeTable,  item, normalCount, fraudCount)) }
//
//    NfcFeature.numericColumns.map { x => 
//       val item="woe_"+x
//      println(x + " count:\t " + getValueCount(hc,woeTable,  item) + "\tIV:\t" + getIVByColumn(hc,woeTable, item, normalCount, fraudCount)) }

  }

}