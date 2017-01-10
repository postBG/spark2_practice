package lab.streaming.structured

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network.
 *  
 * 1. Socket Server(Netcat) 실행....
 *   1.1 CentOS7-14 SSH 연결(MobaXterm)
 *   1.2 # cd /kikang/spark-2.0.2-bin-hadoop2.7
 *   1.3 # yum install nc		//--Netcat(nc) 설치....(필요시....)
 *   1.4 # nc -lk 9999				//--Netcat 실행(port 9999)....   
 *   1.5 # Netcat 콘솔에 데이터 입력....	//--Netcat 데이터 송신....
 *   
 * 2. IDE에서 local 실행.... + Netcat 데이터 송신....
 *   2.1 run : Run As > Scala Application
 *   2.2 # Netcat 콘솔에 데이터 입력....	//--Netcat 데이터 송신....
 *   
 */
object StructuredNetworkWordCount {
  def main(args: Array[String]) {
    
    lab.common.config.Config.setHadoopHOME
    
    val host = "CentOS7-14"
    val port = 9999

    val spark = SparkSession.builder
                                   .master("local[*]")
                                   .appName("StructuredNetworkWordCount")
                                   .config("spark.sql.warehouse.dir", "file:///C:/Scala_IDE_for_Eclipse/eclipse/workspace/Spark2.0.2_Edu_Lab/spark-warehouse")
                                   .getOrCreate()
    import spark.implicits._

    //--Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
                        .format("socket")
                        .option("host", host)
                        .option("port", port)
                        .load()
                        .as[String]

    //--Split the lines into words
    val words = lines.flatMap(_.split(" "))

    //--Generate running word count
    val wordCounts = words.groupBy("value").count()

    //--Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
                                  .outputMode("complete")
                                  .format("console")
                                  .start()  //--like ssc.start()....

    query.awaitTermination()  //--like ssc.awaitTermination()....
  }
}
