package lab.streaming.custom.receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.TaskContext

/**
 * 1. CustomReceiver 구현....
 *   1.1 extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) => 스토리지 레벨에 따른 변화 확인(로컬 실행....)
 *   1.2 onStart() 구현 => store(String)
 *   1.3 onStop() 구현
 * 
 * 2. Socket Server(Netcat) 실행....
 *   2.1 CentOS7-14 SSH 연결(MobaXterm)
 *   2.2 # cd /kikang/spark-2.0.2-bin-hadoop2.7
 *   2.3 # yum install nc		//--Netcat(nc) 설치....(필요시....)
 *   2.4 # nc -lk 9999				//--Netcat 실행(port 9999)....   
 *   2.5 # Netcat 콘솔에 데이터 입력....	//--Netcat 데이터 송신....
 *   
 * 3. IDE에서 local 실행.... + Netcat 데이터 송신....
 *   3.1 run : Run As > Scala Application
 *   3.2 # Netcat 콘솔에 데이터 입력....	//--Netcat 데이터 송신....
 *       
 * 4. spark-submit으로 remote 실행....
 *   4.1 build : Spark2.0.2_Edu_Lab > Run As > Maven install
 *   4.2 remote upload : CentOS7-14 SSH 연결(MobaXterm) > # cd /kikang/spark-2.0.2-bin-hadoop2.7/dev/app > # Spark2.0.2_Edu_Lab-0.0.1-SNAPSHOT-jar-with-dependencies.jar 파일 업로드
 *   4.3 run : CentOS7-14 SSH 연결(MobaXterm) > # cd /kikang/spark-2.0.2-bin-hadoop2.7 > # ./bin/spark-submit --master spark://CentOS7-14:7077 --class lab.streaming.custom.receiver.CustomReceiver ./dev/app/Spark2.0.2_Edu_Lab-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 * 
 */
object CustomReceiver {
  def main(args: Array[String]) {    
    
    var master = "local[*]"
    var host = "CentOS7-14"
    var port = 9999
    var batchSize = 1000L
    if(args.size == 1) {
      master = args(0).toString()
    } else if(args.size == 2) {
      master = args(0).toString()
      host = args(1).toString()
    } else if(args.size == 3) {
      master = args(0).toString()
      host = args(1).toString()
      port = args(2).toInt
    } else if(args.size == 4) {
      master = args(0).toString()
      host = args(1).toString()
      port = args(2).toInt
      batchSize = args(3).toLong
    }     
    println("master : " + master)
    println("host : " + host)
    println("port : " + port)
    println("batchSize : " + batchSize)
    
    val sparkConf=new SparkConf().setAppName("CustomReceiver")
    
    try {      
      println("[Before] spark.master : " + sparkConf.get("spark.master"))
    } catch {
      case e: java.util.NoSuchElementException => sparkConf.setMaster(master)
    }
    println("[After] spark.master : " + sparkConf.get("spark.master"))
    
    
    //--StreamingContext 생성....
    val ssc = new StreamingContext(sparkConf, Milliseconds(batchSize))
    
    //--CustomReceiver를 이용하여 ReceiverInputDStream 생성....
    val lines = ssc.receiverStream(new CustomReceiver(host, port))    //--ssc.receiverStream(Receiver)....
    
    //--Word Count 실행 후 출력....
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    
    //--Word Count 내림차순 정렬 후 상위 5개 (단어, 개수) 출력....with 배치타임....+ 파티션 ID 출력....
    val sortedWordCounts = wordCounts.transform(x => x.sortBy(_._2, false))
    sortedWordCounts.foreachRDD((rdd, time) => {
      //--5개 출력....
      rdd.take(5).foreach(x => println(">>>> [" + time + "]" + x))
      
      //--파티션 ID 출력....
      rdd.foreachPartition(x => {
        val partitionId = TaskContext.get.partitionId()
        println(">>>> Partion ID : " + partitionId)
      })
    })
    
    ssc.start()
    ssc.awaitTermination()
  }
}


class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
  
  /*
   * Setup stuff (start threads, open sockets, etc.) to start receiving data.
   * Must start new thread to receive data, as onStart() must be non-blocking.
   * Call store(...) in those threads to store received data into Spark's memory.
   * Call stop(...), restart(...) or reportError(...) on any thread based on how 
   * different errors need to be handled.
   * See corresponding method documentation for more details 
   */
  def onStart() {
    //--소켓으로 데이터를 수신하는 쓰레드 실행....
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }
  
  /*
   * Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data.
   */
  def onStop() {
  }

  private def receive() {
   var socket: Socket = null
   var userInput: String = null
   try {
     println(">>>> Connecting to " + host + ":" + port)
     socket = new Socket(host, port)
     println(">>>> Connected to " + host + ":" + port)
     val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
     userInput = reader.readLine()
     while(!isStopped && userInput != null) {    //--receiver.isStopped()....
       store(userInput)    //--receiver.store(dataItem: T)....
       userInput = reader.readLine()
     }
     reader.close()
     socket.close()
     println(">>>> Stopped receiving....")
     restart(">>>> Trying to connect again....")
   } catch {
     case e: java.net.ConnectException =>
       restart(">>>> Error connecting to " + host + ":" + port, e)    //--receiver.restart(message: String, error: Throwable)....
     case t: Throwable =>
       restart(">>>> Error receiving data", t)    //--receiver.restart(message: String, error: Throwable)....
   }
  }
}