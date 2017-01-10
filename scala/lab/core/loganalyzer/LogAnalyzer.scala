package lab.core.loganalyzer

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * The following log statistics will be computed:
 *   1. The average, min, and max content size of responses returned from the server.
 *   2. A count of response code's returned. (top count 10)
 *   3. All IPAddresses that have accessed this server more than N times. (N == 10) (top count 10)
 *   4. The top endpoints requested by count. (top count 10)
 * 
 */
object LogAnalyzer {
  def main(args: Array[String]) {
    
    lab.common.config.Config.setHadoopHOME
    
    val logFile = "src/main/resources/apache.access.log"
    
    val sc = new SparkContext("local[2]", "LogAnalyzer")  //--local execution (run on eclipse)....
    val accessLogs = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine).cache()  //--RDD[ApacheAccessLog].cache()

    //--1. The average, min, and max content size of responses returned from the server.
    val contentSizes = accessLogs.map(log => log.contentSize).cache()
    println("1. Content Size Avg: %s, Min: %s, Max: %s".format(
                                                                            contentSizes.reduce(_ + _) / contentSizes.count,
                                                                            contentSizes.min,
                                                                            contentSizes.max))
    val stats = contentSizes.stats
    println("1. Content Size(by StatCounter) Mean: %s, Min: %s, Max: %s".format(
                                                                                                  stats.mean,
                                                                                                  stats.min,
                                                                                                  stats.max))

    //--2. A count of response code's returned. (top count 10)
    val responseCodeToCount = accessLogs.map(log => (log.responseCode, 1))
                                                    .reduceByKey(_ + _)
                                                    .sortBy(r => r._2, false)
                                                    .take(10)
    println(s"""2. Response code counts (top 10): ${responseCodeToCount.mkString("[", " , ", "]")}""")

    //--3. All IPAddresses that have accessed this server more than N times. (N == 10) (top count 10)
    val ipAddresses = accessLogs.map(log => (log.ipAddress, 1))
                                        .reduceByKey(_ + _)
                                        .filter(_._2 > 10)
                                        .sortBy(r => r._2, false)
                                        .map(_._1)
                                        .take(10)
    println(s"""3. IPAddresses > 10 times (top 10): ${ipAddresses.mkString("[", " , ", "]")}""")

    //--4. The top endpoints requested by count. (top count 10)
    val topEndpoints = accessLogs.map(log => (log.endpoint, 1))
                                        .reduceByKey(_ + _)
                                        .top(10)(OrderingUtils.SecondValueOrdering)
    println(s"""4. Top Endpoints (top 10): ${topEndpoints.mkString("[", " , ", "]")}""")
    
    //while(true) {}  //--for debug....
    sc.stop()
  }
}
