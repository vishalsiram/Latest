from pyspark.sql import functions as F

def compute_aggregates(df, category_col, amount_col):
    # Pivot on the category column to compute counts and sums for each category
    agg_df = df.groupBy("payer_account_number", "payer_account_type").pivot(category_col).agg(
        F.count(amount_col).alias("count"),
        F.sum(amount_col).alias("sum")
    )
    
    # Rename columns and add value type column based on the sum
    for category in df.select(category_col).distinct().rdd.flatMap(lambda x: x).collect():
        agg_df = agg_df.withColumnRenamed(category + "_count", "count_" + category) \
                      .withColumnRenamed(category + "_sum", "sum_" + category) \
                      .withColumn("type_" + category, 
                                  F.when((F.col("count_" + category) == 0) | (F.col("sum_" + category).isNull()) | (F.col("sum_" + category) == 0), "No transactions")
                                  .when(F.col("payer_account_type") == "SAVINGS", 
                                        F.when(F.col("sum_" + category) < 5000000, "10")
                                        .when((F.col("sum_" + category) >= 5000000) & (F.col("sum_" + category) < 10000000), "9")
                                        .when((F.col("sum_" + category) >= 10000000) & (F.col("sum_" + category) < 15000000), "8")
                                        .when((F.col("sum_" + category) >= 15000000) & (F.col("sum_" + category) < 20000000), "7")
                                        .when((F.col("sum_" + category) >= 20000000) & (F.col("sum_" + category) < 25000000), "6")
                                        .when((F.col("sum_" + category) >= 25000000) & (F.col("sum_" + category) < 30000000), "5")
                                        .when((F.col("sum_" + category) >= 30000000) & (F.col("sum_" + category) < 35000000), "4")
                                        .when((F.col("sum_" + category) >= 35000000) & (F.col("sum_" + category) < 40000000), "3")
                                        .when((F.col("sum_" + category) >= 40000000) & (F.col("sum_" + category) < 45000000), "2")
                                        .otherwise("1"))
                                  .when(F.col("payer_account_type") == "CURRENT", 
                                        F.when(F.col("sum_" + category) < 15000000, "10")
                                        .when((F.col("sum_" + category) >= 15000000) & (F.col("sum_" + category) < 30000000), "9")
                                        .when((F.col("sum_" + category) >= 30000000) & (F.col("sum_" + category) < 45000000), "8")
                                        .when((F.col("sum_" + category) >= 45000000) & (F.col("sum_" + category) < 60000000), "7")
                                        .when((F.col("sum_" + category) >= 60000000) & (F.col("sum_" + category) < 75000000), "6")
                                        .when((F.col("sum_" + category) >= 75000000) & (F.col("sum_" + category) < 90000000), "5")
                                        .when((F.col("sum_" + category) >= 90000000) & (F.col("sum_" + category) < 105000000), "4")
                                        .when((F.col("sum_" + category) >= 105000000) & (F.col("sum_" + category) < 120000000), "3")
                                        .when((F.col("sum_" + category) >= 120000000) & (F.col("sum_" + category) < 135000000), "2")
                                        .otherwise("1"))
                                  .otherwise("Unknown Type"))
    
    return agg_df

# Compute aggregates for category_level1 and category_level2 
agg_df1 = compute_aggregates(result_df, "category_level1", "payer_amount")
agg_df2 = compute_aggregates(result_df, "category_level2", "payer_amount")

# Join the two aggregated DataFrames 
final_agg_df = agg_df1.join(agg_df2, ["payer_account_number", "payer_account_type"], "outer").fillna(0) 

# Calculate the total transaction sum for each account
total_sum_df = result_df.groupBy("payer_account_number").agg(F.sum("payer_amount").alias("total_transaction_sum_paid_as_payer"))

# Calculate how many times each payer_account_number appears in payee_account_number and the sum of payee_amount
appearance_df = result_df.groupBy("payee_account_number") \
                         .agg(F.count("*").alias("as_a_payee_count"), 
                              F.sum("payee_amount").alias("as_a_payee_sum_recieved"))

# Join with unique account details 
unique_account_details = result_df.select("payer_account_number", "account_holder_name", "account_ifsc", "payer_account_type","payee_account_type","payer_account_type").distinct()
final_df = unique_account_details.join(final_agg_df, ["payer_account_number", "payer_account_type"], "left") \
                                 .join(total_sum_df, ["payer_account_number"], "left") \
                                 .join(appearance_df, unique_account_details.payer_account_number == appearance_df.payee_account_number, "left") \
                                 .drop("payee_account_number") \
                                 .fillna(0)

# Display the result
final_df.show()
--------------------------------
In above code getting below error 

Py4JJavaErrorTraceback (most recent call last)
<ipython-input-20-e6c563422116> in <module>()
     39 
     40 # Compute aggregates for category_level1 and category_level2
---> 41 agg_df1 = compute_aggregates(result_df, "category_level1", "payer_amount")
     42 agg_df2 = compute_aggregates(result_df, "category_level2", "payer_amount")
     43 

<ipython-input-20-e6c563422116> in compute_aggregates(df, category_col, amount_col)
      3 def compute_aggregates(df, category_col, amount_col):
      4     # Pivot on the category column to compute counts and sums for each category
----> 5     agg_df = df.groupBy("payer_account_number", "payer_account_type").pivot(category_col).agg(
      6         F.count(amount_col).alias("count"),
      7         F.sum(amount_col).alias("sum")

/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/pyspark/sql/group.pyc in pivot(self, pivot_col, values)
    216         """
    217         if values is None:
--> 218             jgd = self._jgd.pivot(pivot_col)
    219         else:
    220             jgd = self._jgd.pivot(pivot_col, values)

/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/lib/py4j-0.10.7-src.zip/py4j/java_gateway.py in __call__(self, *args)
   1255         answer = self.gateway_client.send_command(command)
   1256         return_value = get_return_value(
-> 1257             answer, self.gateway_client, self.target_id, self.name)
   1258 
   1259         for temp_arg in temp_args:

/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/pyspark/sql/utils.pyc in deco(*a, **kw)
     61     def deco(*a, **kw):
     62         try:
---> 63             return f(*a, **kw)
     64         except py4j.protocol.Py4JJavaError as e:
     65             s = e.java_exception.toString()

/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/lib/py4j-0.10.7-src.zip/py4j/protocol.py in get_return_value(answer, gateway_client, target_id, name)
    326                 raise Py4JJavaError(
    327                     "An error occurred while calling {0}{1}{2}.\n".
--> 328                     format(target_id, ".", name), value)
    329             else:
    330                 raise Py4JError(

Py4JJavaError: An error occurred while calling o940.pivot.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 44.0 failed 4 times, most recent failure: Lost task 0.3 in stage 44.0 (TID 896, yball1r044ca17.yesbank.com, executor 14): java.io.IOException: Cannot run program "/opt/anaconda/anaconda2/bin/python": error=2, No such file or directory
	at java.lang.ProcessBuilder.start(ProcessBuilder.java:1048)
	at org.apache.spark.api.python.PythonWorkerFactory.startDaemon(PythonWorkerFactory.scala:197)
	at org.apache.spark.api.python.PythonWorkerFactory.createThroughDaemon(PythonWorkerFactory.scala:122)
	at org.apache.spark.api.python.PythonWorkerFactory.create(PythonWorkerFactory.scala:95)
	at org.apache.spark.SparkEnv.createPythonWorker(SparkEnv.scala:118)
	at org.apache.spark.api.python.BasePythonRunner.compute(PythonRunner.scala:111)
	at org.apache.spark.api.python.PythonRDD.compute(PythonRDD.scala:66)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.sql.execution.SQLExecutionRDD.compute(SQLExecutionRDD.scala:55)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:55)
	at org.apache.spark.scheduler.Task.run(Task.scala:123)
	at org.apache.spark.executor.Executor$TaskRunner$$anonfun$11.apply(Executor.scala:413)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1334)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:419)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:750)
Caused by: java.io.IOException: error=2, No such file or directory
	at java.lang.UNIXProcess.forkAndExec(Native Method)
	at java.lang.UNIXProcess.<init>(UNIXProcess.java:247)
	at java.lang.ProcessImpl.start(ProcessImpl.java:134)
	at java.lang.ProcessBuilder.start(ProcessBuilder.java:1029)
	... 50 more

Driver stacktrace:
	at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$failJobAndIndependentStages(DAGScheduler.scala:1928)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:1916)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:1915)
	at scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:59)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:48)
	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:1915)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:951)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:951)
	at scala.Option.foreach(Option.scala:257)
	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:951)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:2149)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2098)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2087)
	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:762)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2079)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2100)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2119)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2144)
	at org.apache.spark.rdd.RDD$$anonfun$collect$1.apply(RDD.scala:990)
	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
	at org.apache.spark.rdd.RDD.withScope(RDD.scala:385)
	at org.apache.spark.rdd.RDD.collect(RDD.scala:989)
	at org.apache.spark.sql.execution.SparkPlan.executeCollect(SparkPlan.scala:309)
	at org.apache.spark.sql.Dataset.org$apache$spark$sql$Dataset$$collectFromPlan(Dataset.scala:3388)
	at org.apache.spark.sql.Dataset$$anonfun$collect$1.apply(Dataset.scala:2788)
	at org.apache.spark.sql.Dataset$$anonfun$collect$1.apply(Dataset.scala:2788)
	at org.apache.spark.sql.Dataset$$anonfun$53.apply(Dataset.scala:3369)
	at org.apache.spark.sql.execution.SQLExecution$$anonfun$withNewExecutionId$1.apply(SQLExecution.scala:80)
	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:127)
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:75)
	at org.apache.spark.sql.Dataset.org$apache$spark$sql$Dataset$$withAction(Dataset.scala:3368)
	at org.apache.spark.sql.Dataset.collect(Dataset.scala:2788)
	at org.apache.spark.sql.RelationalGroupedDataset.pivot(RelationalGroupedDataset.scala:385)
	at org.apache.spark.sql.RelationalGroupedDataset.pivot(RelationalGroupedDataset.scala:317)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
	at py4j.Gateway.invoke(Gateway.java:282)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.GatewayConnection.run(GatewayConnection.java:238)
	at java.lang.Thread.run(Thread.java:748)
Caused by: java.io.IOException: Cannot run program "/opt/anaconda/anaconda2/bin/python": error=2, No such file or directory
	at java.lang.ProcessBuilder.start(ProcessBuilder.java:1048)
	at org.apache.spark.api.python.PythonWorkerFactory.startDaemon(PythonWorkerFactory.scala:197)
	at org.apache.spark.api.python.PythonWorkerFactory.createThroughDaemon(PythonWorkerFactory.scala:122)
	at org.apache.spark.api.python.PythonWorkerFactory.create(PythonWorkerFactory.scala:95)
	at org.apache.spark.SparkEnv.createPythonWorker(SparkEnv.scala:118)
	at org.apache.spark.api.python.BasePythonRunner.compute(PythonRunner.scala:111)
	at org.apache.spark.api.python.PythonRDD.compute(PythonRDD.scala:66)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.sql.execution.SQLExecutionRDD.compute(SQLExecutionRDD.scala:55)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:346)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:310)
	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:55)
	at org.apache.spark.scheduler.Task.run(Task.scala:123)
	at org.apache.spark.executor.Executor$TaskRunner$$anonfun$11.apply(Executor.scala:413)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1334)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:419)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:750)
Caused by: java.io.IOException: error=2, No such file or directory
	at java.lang.UNIXProcess.forkAndExec(Native Method)
	at java.lang.UNIXProcess.<init>(UNIXProcess.java:247)
	at java.lang.ProcessImpl.start(ProcessImpl.java:134)
	at java.lang.ProcessBuilder.start(ProcessBuilder.java:1029)
	... 50 more


