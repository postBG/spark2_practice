package lab.streaming.loganalyzer

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import scala.math._

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
object LogAnalyzerStreamingTotal {
  val WINDOW_LENGTH = new Duration(20 * 1000)
  val SLIDE_INTERVAL = new Duration(5 * 1000)

  val computeRunningSum = (values: Seq[Long], state: Option[Long]) => {
    val currentCount = values.foldLeft(0L)(_ + _)
    val previousCount = state.getOrElse(0L)
    Some(currentCount + previousCount)
  }

  val runningCount = new AtomicLong(0)
  val runningSum = new AtomicLong(0)
  val runningMin = new AtomicLong(Long.MaxValue)
  val runningMax = new AtomicLong(Long.MinValue)

  def main(args: Array[String]) {
    
    lab.common.config.Config.setHadoopHOME
    
    val sparkConf = new SparkConf().setAppName("Log Analyzer Streaming Total").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sc, SLIDE_INTERVAL)

    //--NOTE: Checkpointing must be enabled to use updateStateByKey.
    streamingContext.checkpoint("checkpoint")

    val logLinesDStream = streamingContext.socketTextStream("CentOS7-14", 9999)

    //val accessLogsDStream = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()
    val accessLogsDStream = logLinesDStream.flatMap(x => {
      try {
        Array(ApacheAccessLog.parseLogLine(x))
      } catch {
        case e: Exception => Array[ApacheAccessLog]()
      }
    }).cache()

    val contentSizesDStream = accessLogsDStream.map(log => log.contentSize).cache()
    
    //--1. Calculate statistics based on the content size.
    contentSizesDStream.foreachRDD(rdd => {
      val count = rdd.count()
      if (count > 0) {
        runningCount.getAndAdd(count)
        runningSum.getAndAdd(rdd.reduce(_ + _))        
        runningMin.set(min(runningMin.get(), rdd.min()))
        runningMax.set(max(runningMax.get(), rdd.max()))
      }
      if (runningCount.get() == 0) {
        println("1. Content Size Avg: -, Min: -, Max: -")
      } else {
        println("1. Content Size Avg: %s, Min: %s, Max: %s".format(
          runningSum.get() / runningCount.get(),
          runningMin.get(),
          runningMax.get()
        ))
      }
    })

    //--2. Compute Response Code to Count.
    val responseCodeCountDStream = accessLogsDStream
      .map(log => (log.responseCode, 1L))
      .reduceByKey(_ + _)
    val cumulativeResponseCodeCountDStream = responseCodeCountDStream
      .updateStateByKey(computeRunningSum)
    cumulativeResponseCodeCountDStream.foreachRDD(rdd => {
      val responseCodeToCount = rdd.take(100)
      println(s"""2. Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")
    })
    
    //--3. Any IPAddress that has accessed the server more than 10 times.
    val ipAddressDStream = accessLogsDStream
      .map(log => (log.ipAddress, 1L))
      .reduceByKey(_ + _)
      .updateStateByKey(computeRunningSum)
      .filter(_._2 > 10)
      .map(_._1)
    ipAddressDStream.foreachRDD(rdd => {
      val ipAddresses = rdd.take(100)
      println(s"""3. IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")
    })
    
    //--4. Top Endpoints.
    val endpointCountsDStream = accessLogsDStream
      .map(log => (log.endpoint, 1L))
      .reduceByKey(_ + _)
      .updateStateByKey(computeRunningSum)
    endpointCountsDStream.foreachRDD(rdd => {
      val topEndpoints = rdd.top(10)(OrderingUtils.SecondValueLongOrdering)
      println(s"""4. Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
