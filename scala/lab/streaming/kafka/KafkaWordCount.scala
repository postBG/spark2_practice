package lab.streaming.kafka

import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.TaskContext

/**
 * 1. IDE에서 local 실행....
 *   1.1 run : Run As > Scala Application
 *   1.2 드라이버 웹 UI 확인....(http://localhost:4040)....Streaming탭(Batch Time 링크)/Executors탭(Active Tasks '있음' 확인)/Jobs탭(Receiver '있음' 확인)/Stages(Show Additional Metrics 모두체크, DAG에서 checkpoint,cache 확인)/Storage(Window연산=>Cache사용)....
 *   1.3 체크포인트 디렉토리 변화 확인....
 *   
 * 2. spark-submit으로 remote 실행....
 *   2.1 build : Spark2.0.2_Edu_Lab > Run As > Maven install
 *   2.2 remote upload : CentOS7-14 SSH 연결(MobaXterm) > # cd /kikang/spark-2.0.2-bin-hadoop2.7/dev/app > Spark2.0.2_Edu_Lab-0.0.1-SNAPSHOT-jar-with-dependencies.jar 파일 업로드
 *   2.3 run : CentOS7-14 SSH 연결(MobaXterm) > # cd /kikang/spark-2.0.2-bin-hadoop2.7 > # ./bin/spark-submit --master spark://CentOS7-14:7077 --class lab.streaming.kafka.KafkaWordCount ./dev/app/Spark2.0.2_Edu_Lab-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 * 
 */
object KafkaWordCount {
  def main(args: Array[String]) {
    
    lab.common.config.Config.setHadoopHOME
    
    var master = "local[*]"
    if(args.size == 1) {
      master = args(0).toString()
    }    
    println("master : " + master)
    
    val Array(zkQuorum, group, topics, numThreads) = Array("CentOS7-14:2181", "group_02", "log", "4")
    
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    
    try {      
      println("[Before] spark.master : " + sparkConf.get("spark.master"))
    } catch {
      case e: java.util.NoSuchElementException => sparkConf.setMaster(master)
    }
    println("[After] spark.master : " + sparkConf.get("spark.master"))
    
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //val ssc = new StreamingContext(sparkConf, Milliseconds(500))
    ssc.checkpoint("checkpoint")
    
    var value = 1L
    
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap    
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)    //--(key, message) => message....
    val words = lines.flatMap(_.split(" "))
    /*
     * reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks])....
     * 
     * A more efficient version of the reduceByKeyAndWindow() 
     * where the reduce value of each window is calculated incrementally 
     * using the reduce values of the previous window. 
     * This is done by reducing the new data that enters the sliding window, 
     * and “inverse reducing” the old data that leaves the window.
     * Note that checkpointing must be enabled for using this operation.
     */
    val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)    //--reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks])....
    val addedWordCounts = wordCounts.map(x => ("["+value+"]", x._1, x._2))
    addedWordCounts.print()
    
    ssc.start()
    
    while(true) {
      value += 1L
      Thread.sleep(2000)
    }
    
    ssc.awaitTermination()
    
    value = 1000000L
  }
}
