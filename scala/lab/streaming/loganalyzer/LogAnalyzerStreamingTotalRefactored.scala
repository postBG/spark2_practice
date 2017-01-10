package lab.streaming.loganalyzer

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

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
object LogAnalyzerStreamingTotalRefactored {
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

  val contentSizeStats = (accessLogRDD: RDD[ApacheAccessLog]) => {
    val contentSizes = accessLogRDD.map(log => log.contentSize).cache()
    (contentSizes.count(), contentSizes.reduce(_ + _),
      contentSizes.min, contentSizes.max)
  }

  val responseCodeCount = (accessLogRDD: RDD[ApacheAccessLog]) => {
    accessLogRDD.map(log => (log.responseCode, 1L)).reduceByKey(_ + _)
  }

  val ipAddressCount = (accessLogRDD: RDD[ApacheAccessLog]) =>  {
    accessLogRDD.map(log => (log.ipAddress, 1L)).reduceByKey(_ + _)
  }

  val filterIPAddress = (ipAddressCount: RDD[(String, Long)]) => {
    ipAddressCount.filter(_._2 > 10).map(_._1)
  }

  val endpointCount = (accessLogRDD: RDD[ApacheAccessLog]) => {
    accessLogRDD.map(log => (log.endpoint, 1L)).reduceByKey(_ + _)
  }

  def main(args: Array[String]) {
    
    lab.common.config.Config.setHadoopHOME
    
    val sparkConf = new SparkConf().setAppName("Log Analyzer Streaming Total Refactored").setMaster("local[*]")
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

    //--1. Calculate statistics based on the content size.
    accessLogsDStream.foreachRDD(rdd => {
      val count = rdd.count()
      if (count > 0) {
        val currentContentSizes = contentSizeStats(rdd)
        runningCount.getAndAdd(currentContentSizes._1)
        runningSum.getAndAdd(currentContentSizes._2)
        runningMin.set(currentContentSizes._3)
        runningMax.set(currentContentSizes._4)
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
      .transform(responseCodeCount)
    val cumulativeResponseCodeCountDStream = responseCodeCountDStream
      .updateStateByKey(computeRunningSum)

    cumulativeResponseCodeCountDStream .foreachRDD(rdd => {
      val responseCodeToCount = rdd.take(100)
      println( s"""2. Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")
    })
    
    //--3. Any IPAddress that has accessed the server more than 10 times.
    val ipAddressDStream = accessLogsDStream
      .transform(ipAddressCount)
      .updateStateByKey(computeRunningSum)
      .transform(filterIPAddress)
    ipAddressDStream.foreachRDD(rdd => {
      val ipAddresses = rdd.take(100)
      println( s"""3. IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")
    })
    
    //--4. Top Endpoints.
    val endpointCountsDStream = accessLogsDStream
      .transform(endpointCount)
      .updateStateByKey(computeRunningSum)
    endpointCountsDStream.foreachRDD(rdd => {
      val topEndpoints = rdd.top(10)(OrderingUtils.SecondValueLongOrdering)
      println( s"""4. Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
