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


object get_from_HDFS { 
  def main(args: Array[String]) { 
  
  }
    
     
  def get_origin_DF(ss: SparkSession, startdate:String, enddate:String):DataFrame = {
    val sc = ss.sparkContext
    
    var All_DF: DataFrame = null
    //until和Range是左闭右开，1是包含的，10是不包含。而to是左右都包含。  for(i <- 0 until 10);  var r = Range(1,10,2);  默认步长1
    
    var start = constUtil.date_to_num_Map(startdate)
    var end = constUtil.date_to_num_Map(enddate)
    
    for(i <- start to end) {
       val filename = "/user/hddtmn/in_common_his_trans/" + constUtil.dateMap(i) + "_correct"
       println(filename)
       val tmpRdd = sc.textFile(filename).map{str=>
           var tmparr = str.split("\",\"")         
           tmparr = tmparr.map { x => x.toString()}    
           Row.fromSeq(tmparr.toSeq)
       }
   
       var tmp_DF = ss.createDataFrame(tmpRdd, constUtil.schema_251)
    
       val udf_pdate = udf[String, String]{xstr => constUtil.dateMap(i)}
       tmp_DF = tmp_DF.withColumn("pdate", udf_pdate(tmp_DF("pri_key")))
       //tmp_DF.show(5)
       if(i==start)
         All_DF = tmp_DF
       else
         All_DF = All_DF.unionAll(tmp_DF)
    } 
       All_DF
   }
    
  
    def get_spilit_DF(ss: SparkSession, startdate:String, enddate:String):DataFrame = {
        var AllData = get_origin_DF(ss, startdate, enddate)
			  AllData.na.fill("isNull")
			
			val udf_substring_BK = udf[String, String]{xstr => 
				if(xstr.length()==8)
				  xstr.substring(0, 4)
				else
				  xstr
			  }
			
			val udf_substring_RG = udf[String, String]{xstr => 
				if(xstr.length()==8)
				  xstr.substring(4, 8)
				else
				  xstr
			  }
			
			AllData = AllData.withColumn("acq_ins_id_cd_BK", udf_substring_BK(AllData("acq_ins_id_cd"))) 
			AllData = AllData.withColumn("fwd_ins_id_cd_BK", udf_substring_BK(AllData("fwd_ins_id_cd")))
			AllData = AllData.withColumn("rcv_ins_id_cd_BK", udf_substring_BK(AllData("rcv_ins_id_cd")))
			AllData = AllData.withColumn("iss_ins_id_cd_BK", udf_substring_BK(AllData("iss_ins_id_cd")))
			AllData = AllData.withColumn("related_ins_id_cd_BK", udf_substring_BK(AllData("related_ins_id_cd")))
			AllData = AllData.withColumn("acpt_ins_id_cd_BK", udf_substring_BK(AllData("acpt_ins_id_cd")))
			AllData = AllData.withColumn("settle_fwd_ins_id_cd_BK", udf_substring_BK(AllData("settle_fwd_ins_id_cd")))
			AllData = AllData.withColumn("settle_rcv_ins_id_cd_BK", udf_substring_BK(AllData("settle_rcv_ins_id_cd")))
			AllData = AllData.withColumn("acct_ins_id_cd_BK", udf_substring_BK(AllData("acct_ins_id_cd")))
			AllData = AllData.withColumn("acq_ins_id_cd_RG", udf_substring_RG(AllData("acq_ins_id_cd"))) 
			AllData = AllData.withColumn("fwd_ins_id_cd_RG", udf_substring_RG(AllData("fwd_ins_id_cd")))
			AllData = AllData.withColumn("rcv_ins_id_cd_RG", udf_substring_RG(AllData("rcv_ins_id_cd")))
			AllData = AllData.withColumn("iss_ins_id_cd_RG", udf_substring_RG(AllData("iss_ins_id_cd")))
			AllData = AllData.withColumn("related_ins_id_cd_RG", udf_substring_RG(AllData("related_ins_id_cd")))
			AllData = AllData.withColumn("acpt_ins_id_cd_RG", udf_substring_RG(AllData("acpt_ins_id_cd")))
			AllData = AllData.withColumn("settle_fwd_ins_id_cd_RG", udf_substring_RG(AllData("settle_fwd_ins_id_cd")))
			AllData = AllData.withColumn("settle_rcv_ins_id_cd_RG", udf_substring_RG(AllData("settle_rcv_ins_id_cd")))
			AllData = AllData.withColumn("acct_ins_id_cd_RG", udf_substring_RG(AllData("acct_ins_id_cd")))
     
			AllData
   }

    
    def get_filled_DF(ss: SparkSession, startdate:String, enddate:String):DataFrame = {
       var AllData = get_spilit_DF(ss, startdate, enddate)
       val DisperseArr =  constUtil.DisperseArr
       
       val udf_replaceEmpty = udf[String, String]{xstr => 
        if(xstr.isEmpty())
          "NANs"
        else
          xstr
       }
       
       for(oldcol <- DisperseArr){
        val newcol = oldcol + "_filled" 
        AllData = AllData.withColumn(newcol, udf_replaceEmpty(AllData(oldcol)))
       }
     
       AllData
    }
}