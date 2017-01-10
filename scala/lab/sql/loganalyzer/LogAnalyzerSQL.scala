package lab.sql.loganalyzer

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}

/**
 * The following log statistics will be computed:
 *   1. The average, min, and max content size of responses returned from the server.
 *   2. A count of response code's returned. (top count 10)
 *   3. All IPAddresses that have accessed this server more than N times. (N == 10) (top count 10)
 *   4. The top endpoints requested by count. (top count 10)
 * 
 */
object LogAnalyzerSQL {
  def main(args: Array[String]) {
    
    lab.common.config.Config.setHadoopHOME
    
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    
    val spark = SparkSession.builder()
                                   .master("local[2]")
                                   .appName("LogAnalyzerSQL")
                                   .config("spark.sql.warehouse.dir", "file:///C:/Scala_IDE_for_Eclipse/eclipse/workspace/Spark2.0.0_Edu_Lab/spark-warehouse")
                                   //.config("spark.sql.warehouse.dir", warehouseLocation)
                                   .getOrCreate()
    
    //--For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    
    val logFile = "src/main/resources/apache.access.log"
    
    val accessLogs = spark.sparkContext.textFile(logFile).map(ApacheAccessLog.parseLogLine).toDF()
    accessLogs.createOrReplaceTempView("accessLogs")
    spark.table("accessLogs").cache()
    
    //--1. The average, min, and max content size of responses returned from the server.
    val contentSizeStats = spark.sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM accessLogs")
                                      .first()
    println("1. Content Size Avg: %s, Min: %s, Max: %s".format(
                                                                            contentSizeStats.getLong(0) / contentSizeStats.getLong(1),
                                                                            contentSizeStats(2),
                                                                            contentSizeStats(3)))

    //--2. A count of response code's returned. (top count 10)
    val responseCodeToCount = spark.sql("SELECT responseCode, COUNT(*) AS total FROM accessLogs GROUP BY responseCode ORDER BY total DESC LIMIT 10")
                                              .map(row => (row.getInt(0), row.getLong(1)))
                                              .collect()
    println(s"""2. Response code counts (top 10): ${responseCodeToCount.mkString("[", " , ", "]")}""")

    //--3. All IPAddresses that have accessed this server more than N times. (N == 10) (top count 10)
    val ipAddresses =spark.sql("SELECT ipAddress, COUNT(*) AS total FROM accessLogs GROUP BY ipAddress HAVING total > 10 ORDER BY total DESC LIMIT 10")
                                .map(row => row.getString(0))
                                .collect()
    println(s"""3. IPAddresses > 10 times (top 10): ${ipAddresses.mkString("[", " , ", "]")}""")
    
    //--4. The top endpoints requested by count. (top count 10)
    val topEndpoints = spark.sql("SELECT endpoint, COUNT(*) AS total FROM accessLogs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
                                  .map(row => (row.getString(0), row.getLong(1)))
                                  .collect()
    println(s"""4. Top Endpoints (top 10): ${topEndpoints.mkString("[", " , ", "]")}""")
    
    //while(true) {}  //--for debug....
    spark.stop()
  }
}
