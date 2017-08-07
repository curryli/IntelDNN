package IntelUtil

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap		


object varUtil { 
  def main(args: Array[String]) { 
  }
		
  val startdate = "20161001"
  val enddate = "20161031"
  val rangeDir = "xrli/IntelDNN/CashOut/201610/" 
  
  val idx_model = "xrli/IntelDNN/CashOut/models/index_Model_1101"
			
}