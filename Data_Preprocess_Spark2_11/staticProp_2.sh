spark3-submit \
--class PropMap.Save_IndexerPipeLine \
--master yarn \
--deploy-mode cluster \
--queue root.spark \
--driver-memory 6g \
--executor-memory 6G \
--num-executors 100 \
--conf spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hive/auxlib/* \
--conf spark.driver.extraClassPath=/opt/cloudera/parcels/CDH/lib/hive/auxlib/*  \
--conf spark.yarn.executor.memoryOverhead=1G \
IntelDNN2.jar 
