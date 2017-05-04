spark3-submit \
--class PropMap.compare_sparksql \
--master yarn \
--deploy-mode cluster \
--queue root.queue2 \
--driver-memory 3g \
--executor-memory 4G \
--num-executors 100 \
--conf spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hive/auxlib/* \
--conf spark.driver.extraClassPath=/opt/cloudera/parcels/CDH/lib/hive/auxlib/*  \
IntelDNN.jar 
