package lab.streaming.structured

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network over a
 * sliding window of configurable duration. Each line from the network is tagged
 * with a timestamp that is used to determine the windows into which it falls.
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
object StructuredNetworkWordCountWindowed {

  def main(args: Array[String]) {
    
    lab.common.config.Config.setHadoopHOME
    
    val host = "CentOS7-14"
    val port = 9999
    val windowSize = 10  //--<window duration> gives the size of window, specified as integer number of seconds
    val slideSize = 5        //--<slide duration> gives the amount of time successive windows are offset from one another, given in the same units as above. <slide duration> should be less than or equal to <window duration>. If the two are equal, successive windows have no overlap. If <slide duration> is not provided, it defaults to <window duration>.
    
    if (slideSize > windowSize) {
      System.err.println("<slide duration> must be less than or equal to <window duration>")
    }
    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"

    val spark = SparkSession.builder
                                   .master("local[*]")
                                   .appName("StructuredNetworkWordCountWindowed")
                                   .config("spark.sql.warehouse.dir", "file:///C:/Scala_IDE_for_Eclipse/eclipse/workspace/Spark2.0.2_Edu_Lab/spark-warehouse")
                                   .getOrCreate()
    import spark.implicits._

    //--Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
                        .format("socket")
                        .option("host", host)
                        .option("port", port)
                        .option("includeTimestamp", true)
                        .load()
                        .as[(String, Timestamp)]

    //--Split the lines into words, retaining timestamps
    val words = lines.flatMap(line => line._1.split(" ").map(word => (word, line._2))).toDF("word", "timestamp")

    //--Group the data by window and word and compute the count of each group
    val windowedCounts = words.groupBy(
      window($"timestamp", windowDuration, slideDuration), $"word"
    ).count()
    //.orderBy($"window".desc, $"word".asc)
    .sort($"window".desc, $"word".asc)

    //--Start running the query that prints the windowed word counts to the console
    val query = windowedCounts.writeStream
                                        .outputMode("complete")
                                        .format("console")
                                        .option("truncate", "false")
                                        .start()  //--like ssc.start()....

    query.awaitTermination()  //--like ssc.awaitTermination()....
  }
}
