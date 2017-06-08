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


object funUtil { 
  def main(args: Array[String]) { 
  }
		
  val udf_Map0 = udf[Int, String]{xstr => 0}
  val udf_Map1 = udf[Int, String]{xstr => 1}
	
  
			
}