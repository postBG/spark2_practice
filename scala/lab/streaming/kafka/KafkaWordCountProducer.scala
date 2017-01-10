package lab.streaming.kafka

import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
 * 1. Kafka 다운로드....
 *   1.1 # cd /kikang
 *   1.2 # wget http://mirror.apache-kr.org/kafka/0.8.2.2/kafka_2.11-0.8.2.2.tgz
 *   1.3 # tar xvfz kafka_2.11-0.8.2.2.tgz
 * 
 * 2. Zookeeper 설정....(Kafka 내부 포함 Zookeeper 사용....)
 *   2.1 # cd /kikang/kafka_2.11-0.8.2.2/config
 *   2.2 # vi zookeeper.properties
 *          "i" 키 입력 => 입력모드 전환.... 키보드 화살표로 이동하여 필요 정보 입력 및 수정....
 *          dataDir=/kikang/kafka_2.11-0.8.2.2/zookeeper		//--기존 정보 수정....dataDir=/tmp/zookeeper
 *          server.1=CentOS7-14:2889:3889											//--신규 추가....
 *          "Esc" 키 입력 => 입력모드에서 나오기....
 *          ":wq" 키 입력 => 저장 후 vi 종료.... 
 *   2.3 # cat zookeeper.properties 	//--수정 내용 확인....
 *   2.4 # cd /kikang/kafka_2.11-0.8.2.2
 *   2.5 # mkdir zookeeper
 *   2.6 # cd zookeeper
 *   2.7 # echo '1' > myid
 *   2.8 # cat myid		//--myid에 '1' 입력 되어 있음 확인....
 *   
 * 3. Kafka 설정....
 *   3.1 # cd /kikang/kafka_2.11-0.8.2.2/config
 *   3.2 # vi server.properties
 *          "i" 키 입력 => 입력모드 전환.... 키보드 화살표로 이동하여 필요 정보 입력 및 수정.... 
 *          log.dirs=/kikang/kafka_2.11-0.8.2.2/logs		//--기존 정보 수정....log.dirs=/tmp/kafka-logs
 *          delete.topic.enable=true		//--토픽 삭제 가능하도록 설정....//--신규 추가....
 *          "Esc" 키 입력 => 입력모드에서 나오기....
 *          ":wq" 키 입력 => 저장 후 vi 종료.... 
 *   3.3 # cat server.properties //--수정 내용 확인....
 *   3.4 # cd /kikang/kafka_2.11-0.8.2.2
 *   3.5 # mkdir logs
 *       
 * 4. Zookeeper 실행....
 *   4.1 # cd /kikang/kafka_2.11-0.8.2.2/bin
 *   4.2 # nohup ./zookeeper-server-start.sh ../config/zookeeper.properties > nohup_zookeeper.out &
 *   4.3 # jps		//--QuorumPeerMain 프로세스 확인....
 *   
 * 5. Kafka 실행....
 *   5.1 # cd /kikang/kafka_2.11-0.8.2.2/bin
 *   5.2 # nohup ./kafka-server-start.sh ../config/server.properties > nohup_kafka.out &	
 *   5.2 # nohup env JMX_PORT=9995 ./kafka-server-start.sh ../config/server.properties > nohup_kafka.out &		//--JMX 포트 지정하여 실행할 경우....
 *   5.3 # jps		//--Kafka 프로세스 확인....
 *   
 * 6. Kafka 토픽 생성....
 *   6.1 # cd /kikang/kafka_2.11-0.8.2.2/bin
 *   6.2 # ./kafka-topics.sh --create --topic log --partitions 4 --replication-factor 1 --zookeeper CentOS7-14:2181	//--"log" 토픽 생성....	
 *   6.3 # ./kafka-topics.sh --list --zookeeper CentOS7-14:2181	//--토픽 리스트 확인....
 *   6.4 # ./kafka-topics.sh --describe --topic log --zookeeper CentOS7-14:2181	//--"log" 토픽 상세 정보 보기....
 *   6.5 # ./kafka-topics.sh --delete --topic log --zookeeper CentOS7-14:2181	//--"log" 토픽 삭제.... 필요시 실행....
 *   6.6 # ./kafka-topics.sh --alter --topic log --partitions 8 --zookeeper CentOS7-14:2181			//--partitions 개수 변경.... //--Option "[replication-factor]" can't be used with option"[alter]"
 *   
 * 7. Kafka Consumer 실행.... (별도 SSH 세션에서 실행....)
 *   7.1 # cd /kikang/kafka_2.11-0.8.2.2/bin
 *   7.2 # ./kafka-console-consumer.sh --topic log --zookeeper CentOS7-14:2181
 *   7.3 # 토픽에 데이터가 들어오면 Consumer가 토픽에서 데이터를 가져와 콘솔에 출력함....
 *   
 * 8. Kafka Producer 실행.... (별도 SSH 세션에서 실행....)
 *   8.1 # cd /kikang/kafka_2.11-0.8.2.2/bin
 *   8.2 # ./kafka-console-producer.sh --sync --topic log --broker-list CentOS7-14:9092
 *   8.3 # 콘솔에 데이터 입력.... => 토픽에 전송됨....
 *   
 * 9. KafkaWordCountProducer 실행....(로컬 실행....)
 *   9.1 run : Run As > Scala Application
 * 
 */
object KafkaWordCountProducer {

  def main(args: Array[String]) {
    
    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = Array("CentOS7-14:9092", "log", "10", "20")
    
    //--Kafka Producer 관련 정보 설정....
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    
    //--KafkaProducer 생성.... 
    val producer = new KafkaProducer[String, String](props)

    //--메시지 전송....
    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString).mkString(" ")

        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }

      Thread.sleep(1000)
      //Thread.sleep(10)
    }
  }

}
