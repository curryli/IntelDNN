package model
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
import org.apache.spark.ml.feature.QuantileDiscretizer
import scala.collection.mutable.HashMap


object Expand_Indexed_RF {
 
  
   val rangedir = IntelUtil.varUtil.rangeDir 
   //var idx_modelname = IntelUtil.varUtil.idx_model
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
      .getOrCreate()
  
    import ss.implicits._
    import ss.sql
 
    val startTime = System.currentTimeMillis(); 
 
     
    var input_dir = rangedir + "Labeled_All"
    var labeledData = IntelUtil.get_from_HDFS.get_Labeled_All(ss, input_dir)// .cache         //.persist(StorageLevel.MEMORY_AND_DISK_SER)//
    labeledData.show(5)
 
    //labeledData = labeledData.sample(false, 0.0001)
    
    println("testData done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
      
    labeledData = Prepare.FeatureEngineer_function.FE_function(ss, labeledData)
    labeledData.show(5)
    
//    var All_cols =  labeledData.columns
//    
//    var Arr_to_idx = IntelUtil.constUtil.DisperseArr
//    
//    val no_idx_arr = All_cols.toSet.diff(Arr_to_idx.+:("pri_acct_no_conv").+:("label").toSet).toList
//    
//    val CatVecArr = Arr_to_idx.map { x => x + "_idx"}
//    //CatVecArr.foreach {println }
//     
//    
////    val pipeline_idx = new Pipeline().setStages(IntelUtil.funUtil.Multi_idx_Pipeline(Arr_to_idx).toArray)
////
////    labeledData = pipeline_idx.fit(labeledData).transform(labeledData)
////    println("idx done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
////    labeledData.show(5)
//    
//    val my_index_Model = PipelineModel.load(idx_modelname)
//   
//    labeledData = my_index_Model.transform(labeledData)
//      
//    println("Index pipeline done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
//    
//    val feature_arr = no_idx_arr.++(CatVecArr)      //加载idx_model时使用
//    println("feature_arr: ", feature_arr.mkString(","))
//    
//    labeledData = labeledData.selectExpr(List("pri_acct_no_conv","label").++(feature_arr):_*) 
//
//    //labeledData.dtypes.foreach(println)
//    labeledData.show(5)
//     //该数组李必须都是doubletype，否则VectorAssembler报错 
//     
//   // val feature_arr = no_idx_arr.++(CatVecArr)    //不加载idx_model时使用
//    
//    
//    
//    println("start change to double")
//
//    var db_list = List("pri_acct_no_conv","label")
//    
//     for(col <- feature_arr){
//        val newcol = col + "_db" 
//        db_list = db_list.:+(newcol)
//        labeledData = labeledData.withColumn(newcol, labeledData.col(col).cast(DoubleType))
//     }
// 
//    println("change to double done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
//    //labeledData.dtypes.foreach(println)
//    //db_list.foreach(println)
//        
//    labeledData = labeledData.na.fill(-1.0)   // 因为这里填的是-1.0，是double类型，所以  好像只能对double类型的列起作用。  如果填充“1”，那么只对String类型起作用。
//    //labeledData.show()
//    
//    labeledData = labeledData.selectExpr(db_list:_*).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    labeledData.show()
//    
//    //labeledData.rdd.map(_.mkString(",")).saveAsTextFile(rangedir + "FE_db")
//    println("Saved FE_db done:   ",labeledData.columns.mkString(","))
//     
//    val assembler = new VectorAssembler()
//      .setInputCols(db_list.slice(2, db_list.length).toArray)
//      .setOutputCol("featureVector")
// 
//     
////        val normalizer = new Normalizer().setInputCol("features_Cat").setOutputCol("normFeatures")     //默认是L2
////    val l2NormData = normalizer.transform(labeledData)
////    println("Normalize dataframe")
////    l2NormData.show(10)
//    
//    
//    val label_indexer = new StringIndexer()
//     .setInputCol("label")
//     .setOutputCol("label_idx")
//     .fit(labeledData)  
//       
//      
//    val rfClassifier = new RandomForestClassifier()
//        .setLabelCol("label_idx")
//        .setFeaturesCol("featureVector")
//        .setNumTrees(200)
//        .setMaxBins(10000)
//        .setMinInstancesPerNode(10)
//        .setThresholds(Array(100,1))
//        //为每个分类设置一个阈值，参数的长度必须和类的个数相等。最终的分类结果会是p/t最大的那个分类，其中p是通过Bayes计算出来的结果，t是阈值。 
//        //这对于训练样本严重不均衡的情况尤其重要，比如分类0有200万数据，而分类1有2万数据，此时应用new NaiveBayes().setThresholds(Array(100.0,1.0))    这里t1=100  t2=1
//     
//      
//      val Array(trainingData, testData) = labeledData.randomSplit(Array(0.8, 0.2))  
//        
//      val pipeline = new Pipeline().setStages(Array(assembler,label_indexer, rfClassifier))
//      
//      val model = pipeline.fit(trainingData)
//      
//      println("RF done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )
//       
//       
//      val predictionResult = model.transform(testData)
//        
//      val eval_result = IntelUtil.funUtil.get_CF_Matrix(predictionResult)
//       
//      println(eval_result) 
//      
//     println("Current Precision_P is: " + eval_result.Precision_P)
//     println("Current Recall_P is: " + eval_result.Recall_P)
//     
     
 
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
   
     
  }
  
  
  
    
}