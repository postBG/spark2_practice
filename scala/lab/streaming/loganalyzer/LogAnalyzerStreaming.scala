package lab.streaming.loganalyzer

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.SparkContext._

/**
 * 1. Socket Server(Netcat) 실행....
 *   1.1 CentOS7-14 SSH 연결(MobaXterm)
 *   1.2 # cd /kikang/spark-2.0.2-bin-hadoop2.7
 *   1.3 # yum install nc		//--Netcat(nc) 설치....(필요시....)
 *   1.4 # nc -lk 9999				//--Netcat 실행(port 9999)....   
 *   1.5 # Netcat 콘솔에 데이터 입력....	//--Netcat 데이터 송신....
 *   
 * 2. IDE에서 local 실행.... + Netcat 데이터 송신....
 *   2.1 run : Run As > Scala Application
 *   2.2 # Netcat 콘솔에 데이터 입력....	//--Netcat 데이터 송신....=> ApacheAccessLog 메시지 송신....
 *   
 */
object LogAnalyzerStreaming {
  val WINDOW_LENGTH = new Duration(20 * 1000)
  val SLIDE_INTERVAL = new Duration(5 * 1000)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Log Analyzer Streaming").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sc, SLIDE_INTERVAL)

    val logLinesDStream = streamingContext.socketTextStream("CentOS7-14", 9999)

    //val accessLogsDStream = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()
    val accessLogsDStream = logLinesDStream.flatMap(x => {
      try {
        Array(ApacheAccessLog.parseLogLine(x))
      } catch {
        case e: Exception => Array[ApacheAccessLog]()
      }
    }).cache()
    val windowDStream = accessLogsDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL)

    windowDStream.foreachRDD(accessLogs => {
      if (accessLogs.count() == 0) {
        println("No access log received in this time interval")
      } else {
        //--1. Calculate statistics based on the content size.
        val contentSizes = accessLogs.map(log => log.contentSize).cache()
        println("1. Content Size Avg: %s, Min: %s, Max: %s".format(
          contentSizes.reduce(_ + _) / contentSizes.count,
          contentSizes.min,
          contentSizes.max
        ))

        //--2. Compute Response Code to Count.
        val responseCodeToCount = accessLogs
          .map(log => (log.responseCode, 1))
          .reduceByKey(_ + _)
          .take(100)
        println( s"""2. Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

        //--3. Any IPAddress that has accessed the server more than 10 times.
        val ipAddresses = accessLogs
          .map(log => (log.ipAddress, 1))
          .reduceByKey(_ + _)
          .filter(_._2 > 10)
          .map(_._1)
          .take(100)
        println( s"""3. IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

        //--4. Top Endpoints.
        val topEndpoints = accessLogs
          .map(log => (log.endpoint, 1))
          .reduceByKey(_ + _)
          .top(10)(OrderingUtils.SecondValueOrdering)
        println( s"""4. Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
