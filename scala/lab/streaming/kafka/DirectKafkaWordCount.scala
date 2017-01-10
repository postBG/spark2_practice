package lab.streaming.kafka

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext


/**
 * 1. IDE에서 local 실행....
 *   1.1 run : Run As > Scala Application
 *   1.2 드라이버 웹 UI 확인....(http://localhost:4040)....Streaming탭(Batch Time 링크 > Metadata:topic,partition,offsets)/Executors탭(Active Tasks '없음' 확인)/Jobs탭(Receiver '없음' 확인)/Stages(Show Additional Metrics 모두체크)/Storage(Cache없음)....
 *   1.3 체크포인트 디렉토리 변화 확인....
 *   
 * 2. spark-submit으로 remote 실행....
 *   2.1 build : Spark2.0.2_Edu_Lab > Run As > Maven install
 *   2.2 remote upload : CentOS7-14 SSH 연결(MobaXterm) > # cd /kikang/spark-2.0.2-bin-hadoop2.7/dev/app > Spark2.0.2_Edu_Lab-0.0.1-SNAPSHOT-jar-with-dependencies.jar 파일 업로드
 *   2.3 run : CentOS7-14 SSH 연결(MobaXterm) > # cd /kikang/spark-2.0.2-bin-hadoop2.7 > # ./bin/spark-submit --master spark://CentOS7-14:7077 --class lab.streaming.kafka.DirectKafkaWordCount ./dev/app/Spark2.0.2_Edu_Lab-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 * 
 */
object DirectKafkaWordCount {
  def main(args: Array[String]) {
    
    lab.common.config.Config.setHadoopHOME
    
    var master = "local[*]"
    if(args.size == 1) {
      master = args(0).toString()
    }    
    println("master : " + master)
    
    val Array(brokers, topics) = Array("CentOS7-14:9092", "log")

    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    
    try {      
      println("[Before] spark.master : " + sparkConf.get("spark.master"))
    } catch {
      case e: java.util.NoSuchElementException => sparkConf.setMaster(master)
    }
    println("[After] spark.master : " + sparkConf.get("spark.master"))
    
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")  //--Offsets are tracked by Spark Streaming within its checkpoints.... not use Zookeeper....
    
    var value = 1L
     
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    
    //--1. Exactly-once Semantics of receiving the data.... without Write Ahead Logs....is no Receiver.... Kafka is a reliable source....
    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    
    //--Access the Kafka offsets consumed in each batch....
    var offsetRanges = Array[OffsetRange]()
    val messages = directKafkaStream.transform{ rdd => 
       offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges       
       //for (o <- offsetRanges) { println(s">>>> $$ Topic : ${o.topic}, Partition : ${o.partition}, FromOffset : ${o.fromOffset}, UntilOffset : ${o.untilOffset}") }
       rdd
    }
    
    //--2. Exactly-once Semantics of transforming the data....
    val lines = messages.map(_._2)    //--(key, message) => message....
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    val addedWordCounts = wordCounts.map(x => ("["+value+"]", x._1, x._2))
    addedWordCounts.print()
    
    //--3. Exactly-once Semantics of output operations.... A). Idempotent updates.... B). Transactional updates....
    addedWordCounts.foreachRDD((rdd, time) => {
      for (o <- offsetRanges) { println(s">>>> Topic : ${o.topic}, Partition : ${o.partition}, FromOffset : ${o.fromOffset}, UntilOffset : ${o.untilOffset}") }
      rdd.foreachPartition(partitionIterator => {    //--foreachPartition => 파티션 단위로 작업....
        val partitionId = TaskContext.get.partitionId()
        val uniqueId = s"[T]${time.milliseconds}_[P]${partitionId}"
        println(">>>> uniqueId : " + uniqueId)
        //--use this uniqueID to transactionally commit the data in partitionIterator....=> 파티션 단위로 uniqueID로 묶어서 트랜잭션 작업....
      })
    })
    
    ssc.start()
    
    while(true) {
      value += 1L
      Thread.sleep(2000)
    }
    
    ssc.awaitTermination()
    
    value = 1000000L
    println(">>>> value : " + value)
  }
}
