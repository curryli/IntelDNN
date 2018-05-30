package QR_fraud
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.OneHotEncoder


 
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.Vectors  
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import getdata.get_from_hive


object FE {
  

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
    
           
    val startTime = System.currentTimeMillis(); 
     
    var data_division = get_from_hive(hc).cache()
    
    
    val DisperseArr = Array("iss_head", "iss_ins_id_cd", "resp_cd","app_ins_inf","acq_ins_id_cd","mchnt_tp","card_attr","acct_class","app_ins_id_cd","fwd_ins_id_cd","trans_curr_cd","trans_tp","proc_st","ins_pay_mode","up_discount","app_discount","ctrl_rule1","mer_version","app_version","order_type","app_ntf_st","acq_ntf_st","proc_sys","mchnt_back_url","app_back_url","mer_cert_id","mchnt_nm","acq_ins_inf","country_cd","area_cd")
 
    //val DisperseArr = Array("iss_head", "iss_ins_id_cd")
         
    data_division = data_division.na.fill("NULL",DisperseArr);   // null和empty不是一回事
    
    val udf_replaceEmpty = udf[String, String]{xstr => 
        if(xstr.isEmpty())
          "NANs"
        else
          xstr
      }
 
     for(oldcol <- DisperseArr){
        val newcol = oldcol + "_filled" 
        println(oldcol , " count: ", data_division.select(oldcol).distinct().count())
        data_division = data_division.withColumn(newcol, udf_replaceEmpty(data_division(oldcol)))
     }
      
     data_division.show(10)  
     
     var i = 0
     for(oldcol <- DisperseArr){   
        println(i)
        i = i + 1 
        val newcol = oldcol + "_filled" 
        var indexCat = oldcol + "_CatVec"
        var indexer = new StringIndexer().setInputCol(newcol).setOutputCol(indexCat).setHandleInvalid("skip")
        data_division = indexer.fit(data_division).transform(data_division)
      }
      
    data_division.show(10)  
    
    //////////////////////////////////////////////////////
    val CatVecArr = DisperseArr.map { x => x + "_CatVec"}
    
    val sus_Arr = Array("trans_at", "settle_at", "ls_trans_at")
    
    val used_arr = sus_Arr.++(CatVecArr)
    
    //data_division = data_division.selectExpr(used_arr.+:("division"):_*)
    //data_division.show(5)
    
    val assembler1 = new VectorAssembler()
      .setInputCols(used_arr)
      .setOutputCol("featureVector")
     
    data_division = assembler1.transform(data_division)
    println("assembler1 dataframe")
    data_division.show(10) 
      
      
    val normalizer1 = new Normalizer().setInputCol("featureVector").setOutputCol("normFeatures")     //默认是L2
    data_division = normalizer1.transform(data_division)
     
    val laber_indexer = new StringIndexer()
     .setInputCol("label")
     .setOutputCol("label_idx")
     .fit(data_division)  
    
    data_division = laber_indexer.transform(data_division)
    
    println("labeled Normalize dataframe")
    data_division.show(10)
     
    var normal_train = data_division.filter(data_division("division")=== "normal_train")
    var normal_test = data_division.filter(data_division("division")=== "normal_test")
    var fraud_train = data_division.filter(data_division("division")=== "fraud_train")
    var fraud_test = data_division.filter(data_division("division")=== "fraud_test")
   
    val trainingData = normal_train.sample(false, 0.005).unionAll(fraud_train).cache
    val testData = normal_test.unionAll(fraud_test).cache
    
    data_division.unpersist(blocking=false)
    
    println("trainingData.count: ", trainingData.count, " testData.count: ", testData.count)
    
    
//    trainingData.selectExpr(used_arr.+:("label_idx"):_*).rdd.map(_.mkString(",")).saveAsTextFile("xrli/QRfraud/trainingData")
//    testData.selectExpr(used_arr.+:("label_idx"):_*).rdd.map(_.mkString(",")).saveAsTextFile("xrli/QRfraud/testData")
    
     
    println("Save done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
    
    val rfClassifier = new RandomForestClassifier()
        .setLabelCol("label_idx")
        .setFeaturesCol("featureVector")
        .setNumTrees(50)
        .setSubsamplingRate(0.7)
        .setFeatureSubsetStrategy("auto")
        .setThresholds(Array(1000,1))
         
        .setImpurity("gini")
        .setMaxDepth(5)
        .setMaxBins(10000)

        //为每个分类设置一个阈值，参数的长度必须和类的个数相等。最终的分类结果会是p/t最大的那个分类，其中p是通过Bayes计算出来的结果，t是阈值。 
        //这对于训练样本严重不均衡的情况尤其重要，比如分类0有200万数据，而分类1有2万数据，此时应用new NaiveBayes().setThresholds(Array(100.0,1.0))    这里t1=100  t2=1
     
      
          
    val pipeline = new Pipeline().setStages(Array(rfClassifier))
      
    val model = pipeline.fit(trainingData)
     
    println("training done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
       
    val predictionResult = model.transform(testData)
        
    val eval_result = IntelUtil.funUtil.get_CF_Matrix(predictionResult)
     
    println("Current Precision_P is: " + eval_result.Precision_P)
    println("Current Recall_P is: " + eval_result.Recall_P)
     
     
 
    
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label_idx").setMetricName("areaUnderROC")
       
    val accuracy = evaluator.evaluate(predictionResult) //AUC
    
    println("accuracy is: " + accuracy)
    
    
    println("FE done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
  }
   
  
}