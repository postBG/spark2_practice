package lab.sql.window

/**
 * 
 
 
 1. Spark Shell 연결.... Spark SQL for Window....
  
  - # ./bin/spark-shell --master spark://CentOS7-14:7077
  
  - //--Building the customer DataFrame....
  - scala> 
  val customers = sc.parallelize(List(("Alice", "2016-05-01", 50.00),
                                    						("Alice", "2016-05-03", 45.00),
                                    						("Alice", "2016-05-04", 55.00),
                                    						("Bob", "2016-05-01", 25.00),
                                    						("Bob", "2016-05-04", 29.00),
                                    						("Bob", "2016-05-06", 27.00))).
                               						toDF("name", "date", "amountSpent")
  
  - //--Import the window functions....
	- scala> import org.apache.spark.sql.expressions.Window
	- scala> import org.apache.spark.sql.functions._ 
	
	- // Create a window spec....
	- scala> val wSpec1 = Window.partitionBy("name").orderBy("date").rowsBetween(-1, 1)
	
	- //--Calculate the moving average
	- scala> customers.withColumn( "movingAvg",
                                             avg(customers("amountSpent")).over(wSpec1)  ).show()                              
  
  
   +------+------------+---------------+------------+
    |  name|              date|        amountSpent|     movingAvg|
   +------+------------+---------------+------------+
    |    Bob|   2016-05-01|                    25.0|              27.0|
    |    Bob|   2016-05-04|                    29.0|              27.0|
    |    Bob|   2016-05-06|                    27.0|              28.0|
    |   Alice|   2016-05-01|                    50.0|              47.5|
    |   Alice|   2016-05-03|                    45.0|              50.0|
    |   Alice|   2016-05-04|                    55.0|              50.0|
   +------+------------+---------------+------------+
 * 
 */
object SparkShell {
  
}