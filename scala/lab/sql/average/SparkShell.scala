package lab.sql.average

/**
 * 
 
 
 1. Spark Shell 연결.... Spark SQL for Window....
  
  - # ./bin/spark-shell --master spark://CentOS7-14:7077
  
  - //--Dataset JOIN....
  - scala> val people = spark.read.json("examples/src/main/resources/people.json")
  - scala> people.createOrReplaceTempView("people")
  - scala> spark.sql("SELECT name, avg(age) FROM people GROUP BY name").show
 * 
 */
object SparkShell {
  
}