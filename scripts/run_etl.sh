docker exec snaptrip_interview-spark-master-1 \
  spark-submit --master spark://spark-master:7077 \
  --files log4j.properties --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
  --executor-memory 4g /opt/bitnami/spark/scripts/user_etl.py


docker exec snaptrip_interview-spark-master-1 \
  spark-submit --master spark://spark-master:7077 \
  --files log4j.properties --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
  --executor-memory 4g /opt/bitnami/spark/scripts/product_etl.py


docker exec snaptrip_interview-spark-master-1 \
  spark-submit --master spark://spark-master:7077 \
  --files log4j.properties --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
  --executor-memory 4g /opt/bitnami/spark/scripts/order_etl.py