package Prepare
import org.apache.log4j.Level
import org.apache.log4j.Logger
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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import scala.collection.mutable.MutableList
import scala.Range
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.PipelineModel


object SaveLabel_for_test {
    val startdate = "20161001"            //IntelUtil.varUtil.startdate
    val enddate = "20161231"                        //IntelUtil.varUtil.enddate
    val rangedir = IntelUtil.varUtil.testDir                                //IntelUtil.varUtil.rangeDir 
    val usedArr_filled = IntelUtil.constUtil.usedArr.map{x => x + "_filled"}
    
    //可以调整
    val sample_cards_ratio = 0.0005
    val TF_ratio = 1000
    val fraudType = "04"
       
    var fraudType_cards_num = 0L
    var normal_cards_num = 0L
    var fraudType_related_fraud_count = 0L
    
   var idx_modelname = IntelUtil.varUtil.idx_model
     
 
  def main(args: Array[String]): Unit = {

    //屏蔽日志
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR); 
    
    //val sparkConf = new SparkConf().setAppName("spark2SQL")
    val warehouseLocation = "spark-warehouse"
    
    val ss = SparkSession
      .builder()
      .appName("Save_IndexerPipeLine")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.schema.verification", false)
      .config("spark.driver.maxResultSize", "10g")
      .config("spark.debug.maxToStringFields",500)
      .getOrCreate()
  
    import ss.implicits._
    import ss.sql
 
    val startTime = System.currentTimeMillis(); 
    
//    save_fraudType_1(ss)
//    //println("fraudType_cards count is " + fraudType_cards_num)
//    println("step1 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
//      
//    save_sample_cards_2(ss)
//    println("step2 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
//    
//    save_Alldata_bycards_3(ss)
//    println("step3 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
    save_labeled_new_4(ss)
    println("step4 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
    save_FE_5(ss)
    println("step5 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
//    drop_confuse_6(ss)
//    println("step6 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
//    
//      save_train_test_7(ss)
//      println("step7 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
//      
//      drop_FE_test_8(ss)
//      println("step8 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
    
  }
    
    
    def save_fraudType_1(ss: SparkSession): Unit ={
        var fraud_join_Data = IntelUtil.get_from_HDFS.get_fraud_join_DF(ss, startdate, enddate).persist(StorageLevel.MEMORY_AND_DISK_SER)
         
        var fraudType_infraud = fraud_join_Data.filter(fraud_join_Data("fraud_tp")=== fraudType) 
        
        var fraudType_cards = fraudType_infraud.select("pri_acct_no_conv").distinct().persist(StorageLevel.MEMORY_AND_DISK_SER) 
        //fraudType_cards_num = fraudType_cards.count()
        //AllFlow.fraudType_cards_num = fraudType_cards.count()
     
        fraudType_cards.rdd.map(r=>r.getString(0)).saveAsTextFile(rangedir + "fraudType_cards")
     
    	  fraud_join_Data.unpersist(false)
    	  
    	  var fraudType_cards_list = fraudType_cards.rdd.map(r=>r.getString(0)).collect()
    	
        var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000) 
         
        val fraudType_related = AllData.filter(AllData("pri_acct_no_conv").isin(fraudType_cards_list:_*))
        //println("fraudType_related_all_data count is " + fraudType_related.count()) 
         
        var fraudType_fraud = fraudType_related.join(fraudType_infraud, fraudType_related("sys_tra_no")===fraudType_infraud("sys_tra_no"), "leftsemi")
        
        val fraudType_filled = fraudType_fraud.selectExpr(usedArr_filled:_*)
        //fraudType_related_fraud_count = fraudType_filled.count()
        //println("fraudType_related_fraud_data count is " + fraudType_related_fraud_count)
         
        fraudType_filled.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "fraudType_filled")
    }
    
    
    def save_sample_cards_2(ss: SparkSession, sample_ratio: Double = sample_cards_ratio): Unit ={
        var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000) 
  
        val All_sample_cards = AllData.sample(false, sample_ratio, 0).select("pri_acct_no_conv").distinct()//.persist(StorageLevel.MEMORY_AND_DISK_SER) 
        
        All_sample_cards.rdd.map(r=>r.getString(0)).saveAsTextFile(rangedir + "All_sample_cards")
    }
    
    def save_Alldata_bycards_3(ss: SparkSession): Unit ={
        val sc = ss.sparkContext
        val fraudType_cards= sc.textFile(rangedir + "fraudType_cards") 
        var fraudType_cards_num = fraudType_cards.count()
        println("fraudType_cards_num :", fraudType_cards_num)
        
        //var normal_cards_num = fraudType_cards_num * TF_ratio
        
        val All_sample_cards = sc.textFile(rangedir + "All_sample_cards").sample(false,0.05)
        var All_sample_cards_num = All_sample_cards.count()
        println("All_sample_cards_num :", All_sample_cards_num )
        
  
        import ss.implicits._
        var all_cards = All_sample_cards.union(fraudType_cards)
        
        var all_cards_list = all_cards.collect()
        
        println(all_cards_list.size)
        
        
        var AllData_1 = IntelUtil.get_from_HDFS.get_filled_DF(ss, "20161001", "20161031").repartition(5000) 
        val Alldata_by_cards_1 = AllData_1.filter(AllData_1("pri_acct_no_conv").isin(all_cards_list:_*))
        Alldata_by_cards_1.selectExpr(usedArr_filled:_*).rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "Alldata_by_cards_1")
        
        var AllData_2 = IntelUtil.get_from_HDFS.get_filled_DF(ss, "20161101", "20161130").repartition(5000) 
        val Alldata_by_cards_2 = AllData_2.filter(AllData_2("pri_acct_no_conv").isin(all_cards_list:_*))
        Alldata_by_cards_2.selectExpr(usedArr_filled:_*).rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "Alldata_by_cards_2")
        
        var AllData_3 = IntelUtil.get_from_HDFS.get_filled_DF(ss, "20161201", "20161231").repartition(5000) 
        val Alldata_by_cards_3 = AllData_3.filter(AllData_3("pri_acct_no_conv").isin(all_cards_list:_*))
        Alldata_by_cards_3.selectExpr(usedArr_filled:_*).rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "Alldata_by_cards_3")
        
        
//        var used_cards = all_cards.toDF("card")
         
//        var AllData = IntelUtil.get_from_HDFS.get_filled_DF(ss, startdate, enddate).repartition(1000) 
    
//        val used_cards_bd = sc.broadcast(used_cards)
//        val Alldata_by_cards = used_cards_bd.value.join(AllData, (used_cards_bd.value("card")===AllData("pri_acct_no_conv")), "right_outer")//.drop("card")
//        println("Braodcast join done.")
        
        
//        var savepath = rangedir + "Alldata_by_cards"
//        val saveOptions = Map("header" -> "false", "path" -> savepath)
//        Alldata_by_cards.selectExpr(usedArr_filled:_*).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    }
    
    def save_labeled_new_4(ss: SparkSession): Unit ={
        val sc = ss.sparkContext
        val fraudType_cards= sc.textFile(rangedir + "fraudType_cards").cache
        val fraudType_cards_list = fraudType_cards.collect()
        
        val sample_cards= sc.textFile(rangedir + "All_sample_cards").cache
        val all_cards = sample_cards.union(fraudType_cards)
        val all_cards_list = all_cards.collect()
        
        println("all_cards_list done")
       
        var Alldata_by_cards_dir_1 = rangedir + "Alldata_by_cards_1"
        var Alldata_by_cards_filled_1 = IntelUtil.get_from_HDFS.get_processed_DF(ss, Alldata_by_cards_dir_1)
        var Alldata_by_cards_dir_2 = rangedir + "Alldata_by_cards_2"
        var Alldata_by_cards_filled_2 = IntelUtil.get_from_HDFS.get_processed_DF(ss, Alldata_by_cards_dir_2)
        var Alldata_by_cards_dir_3 = rangedir + "Alldata_by_cards_3"
        var Alldata_by_cards_filled_3 = IntelUtil.get_from_HDFS.get_processed_DF(ss, Alldata_by_cards_dir_3)
        
        var Alldata_by_cards_filled = Alldata_by_cards_filled_1.union(Alldata_by_cards_filled_2).union(Alldata_by_cards_filled_3)
        
        var fraudType_dir = rangedir + "fraudType_filled"
        var fraudType_filled = IntelUtil.get_from_HDFS.get_processed_DF(ss, fraudType_dir)
        
 ///////////////////////////////////////////////////////过滤交易次数过多的账号////////////////////////////////////////////////       
        val countdf = Alldata_by_cards_filled.groupBy("pri_acct_no_conv_filled").agg(count("trans_at_filled") as "counts") 
        val filtered_cards = countdf.filter(countdf("counts")<2000)
        filtered_cards.show(5)
        //println("filtered cards count is " + filtered_cards.count)
         
        Alldata_by_cards_filled = Alldata_by_cards_filled.join(filtered_cards, Alldata_by_cards_filled("pri_acct_no_conv_filled")===filtered_cards("pri_acct_no_conv_filled"), "leftsemi")
        //println("Alldata_by_cards_filled count is " + Alldata_by_cards_filled.count) 
        Alldata_by_cards_filled.show(5)
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////    
      
        var normaldata_filled = Alldata_by_cards_filled.except(fraudType_filled)
          
        var fraudType_related_all_data = Alldata_by_cards_filled.filter(Alldata_by_cards_filled("pri_acct_no_conv_filled").isin(fraudType_cards_list:_*))
        //println("normaldata_filled count is " + normaldata_filled.count) 
        
        val udf_Map0 = udf[Double, String]{xstr => 0.0}
        val udf_Map1 = udf[Double, String]{xstr => 1.0}
         
        var NormalData_labeled = normaldata_filled.withColumn("isFraud", udf_Map0(normaldata_filled("trans_md_filled")))
        var fraudType_labeled = fraudType_filled.withColumn("isFraud", udf_Map1(fraudType_filled("trans_md_filled")))
        var LabeledData = fraudType_labeled.unionAll(NormalData_labeled)
        
        LabeledData.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "Labeled_All")
    }
    
    
  def save_FE_5(ss: SparkSession): Unit ={
        val sc = ss.sparkContext
        var input_dir = rangedir + "Labeled_All"
        var labeledData = IntelUtil.get_from_HDFS.get_Labeled_All(ss, input_dir).persist(StorageLevel.MEMORY_AND_DISK_SER)
        labeledData.show(5)
        val cnt_F = labeledData.filter(labeledData("label").===(1)).count()
        val cnt_N = labeledData.filter(labeledData("label").===(0)).count()
        println("cnt_F:", cnt_F, " cnt_N:", cnt_N)
        println("ratio: ", cnt_N/cnt_F)
        
//        (cnt_F:,21189, cnt_N:,34760449)
//        (ratio: ,1640)
        
        
    var new_labeled = FE_new.FE_function(ss, labeledData).persist(StorageLevel.MEMORY_AND_DISK_SER)
      
    println("FeatureEngineer_function done. ")   
     
    var All_cols =  new_labeled.columns
    
    var Arr_to_idx = IntelUtil.constUtil.DisperseArr
    
    val no_idx_arr = All_cols.toSet.diff(Arr_to_idx.+:("pri_acct_no_conv").+:("label").toSet).toList
    

    val CatVecArr = Arr_to_idx.map { x => x + "_idx"}
    
    
    val my_index_Model = PipelineModel.load(idx_modelname)
   
    new_labeled = my_index_Model.transform(new_labeled)
      
    println("Index pipeline done." )
    
    val feature_arr = no_idx_arr.++(CatVecArr)      //加载idx_model时使用
    println("feature_arr: ", feature_arr.mkString(","))
    
    new_labeled = new_labeled.selectExpr(List("pri_acct_no_conv","label").++(feature_arr):_*) 
 
    new_labeled.show(5)
    
    println("start change to double")

    var db_list = List("pri_acct_no_conv","label")
    
     for(col <- feature_arr){
        val newcol = col + "_db" 
        db_list = db_list.:+(newcol)
        new_labeled = new_labeled.withColumn(newcol, new_labeled.col(col).cast(DoubleType))
     }
 
    println("change to double done.")
  
     
    new_labeled = new_labeled.na.fill(-1.0)   // 因为这里填的是-1.0，是double类型，所以  好像只能对double类型的列起作用。  如果填充“1”，那么只对String类型起作用。
   
    new_labeled = new_labeled.selectExpr(db_list:_*) 
    
    println("new_labeled done.")
    //new_labeled = new_labeled.filter(new_labeled("date").===(930L)).persist(StorageLevel.MEMORY_AND_DISK_SER)    //java.lang.OutOfMemoryError: GC overhead limit exceeded
 
      
    new_labeled = new_labeled.repartition(5000)
    
    new_labeled.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "FE_db")
    
//    var savepath = rangedir + "FE_db"
//    val saveOptions = Map("header" -> "false", "path" -> savepath)
//    new_labeled.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
     
    println("Saved FE_db done:   ",new_labeled.columns.mkString(","))
    }
  
  
  
   def drop_confuse_6(ss: SparkSession ): Unit ={
     val sc = ss.sparkContext
     var FE_Data = IntelUtil.get_from_HDFS.get_FE_DF(ss, rangedir + "FE_db").persist(StorageLevel.MEMORY_AND_DISK_SER)
     
     val label= "label" 
     val cardcol= "pri_acct_no_conv"
    
     var Fraud = FE_Data.filter(FE_Data("label")===1)
     val card_c_F = Fraud.select("pri_acct_no_conv").distinct().rdd.map(x=>x.getString(0)).collect()
//     真实比例150000
//     [[29778984     2161]
//     [   12805     4158]]
     
     Fraud = Fraud.sample(false, 0.012)
      
     val Normal = FE_Data.filter(FE_Data("label")===0)
    
     println("Normal count: ", Normal.count())
     
     
 
     val fine_N = Normal.filter(!Normal("pri_acct_no_conv").isin(card_c_F:_*))
     println("fine_N: ", fine_N.count())
    
     val last_data = Fraud.union(fine_N)
     last_data.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "FE_test_real") 
     println("Fraud count: ", Fraud.count(), "fine_N  count: ", fine_N.count())
    
//     (Normal count: ,29841749)
//(fine_N: ,29781145)
//(Fraud count: ,215,fine_N  count: ,29781145)

    }
  
  
   def save_train_test_7(ss: SparkSession ): Unit ={
     val sc = ss.sparkContext
     var FE_Data = IntelUtil.get_from_HDFS.get_FE_DF(ss, rangedir + "FE_db").persist(StorageLevel.MEMORY_AND_DISK_SER)
     
     val label= "label" 
     val cardcol= "pri_acct_no_conv"
    
     var Fraud = FE_Data.filter(FE_Data("label")===1)
      
     val card_c_F = Fraud.select("pri_acct_no_conv").distinct().rdd.map(x=>x.getString(0)).collect()
     
     val Normal = FE_Data.filter(FE_Data("label")===0)
     println("Normal count: ", Normal.count())
     val fine_N = Normal.filter(!Normal("pri_acct_no_conv").isin(card_c_F:_*))
     println("fine_N: ", fine_N.count())
     
//     真实比例150000
//     [[29778984     2161]
//     [   12805     4158]]
     
     var Array(trainFraud, testFraud) = Fraud.randomSplit(Array(0.98, 0.02))
     println("trainFraud: ", trainFraud.count(), "testFraud: ", testFraud.count())
      
     var Array(trainNormal, testNormal) = fine_N.randomSplit(Array(0.05, 0.95))
     println("trainNormal: ", trainNormal.count(), "testNormal: ", testNormal.count())
    
     val train_data = trainFraud.union(trainNormal)
     train_data.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "FE_train_data") 
     
     val test_data = testFraud.union(testNormal)
     test_data.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "FE_test_data")
     
    
//     (fine_N: ,29781145)
//(trainFraud: ,16607,testFraud: ,355)
//(trainNormal: ,1489925,testNormal: ,28291220)
    }
   
   
    def drop_FE_test_8(ss: SparkSession ): Unit ={
     val sc = ss.sparkContext
     var FE_Data = IntelUtil.get_from_HDFS.get_FE_DF(ss, rangedir + "FE_test_data").persist(StorageLevel.MEMORY_AND_DISK_SER)
     
     val label= "label" 
     val cardcol= "pri_acct_no_conv"
    
     var Fraud = FE_Data.filter(FE_Data("label")===1)
     Fraud = Fraud.sample(false, 0.57)
       
     val Normal = FE_Data.filter(FE_Data("label")===0)
       
     val test_data = Fraud.union(Normal)
     test_data.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "FE_test_data_new")
      
    }
  
}