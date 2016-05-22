rm -r /Users/rafaelpossas/Dev/ml-latest/results
/usr/local/Cellar/apache-spark/1.6.1/bin/spark-submit \
--class assignment.MovieStatistics \
--master local \
--num-executors 3 \
target/cc-spark-1.0.jar \
/Users/rafaelpossas/Dev/ml-latest/ \
/Users/rafaelpossas/Dev/ml-latest/results/ \
local
