package lab.streaming.loganalyzer

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Duration}

/**
 * 1. Socket Server(Netcat) 실행....
 *   1.1 CentOS7-14 SSH 연결(MobaXterm)
 *   1.2 # cd /kikang/spark-2.0.2-bin-hadoop2.7
 *   1.3 # yum install nc		//--Netcat(nc) 설치....(필요시....)
 *   1.4 # nc -lk 9999				//--Netcat 실행(port 9999)....   
 *   1.5 # Netcat 콘솔에 데이터 입력....	//--Netcat 데이터 송신....
 *   
 * 2. MySQL 실행....(+ 원격접속 허용....root계정 모든 IP 허용) + IDE에서 local 실행.... + Netcat 데이터 송신....
 *   2.1 MySQL 실행....(+ 원격접속 허용....)
 *     - # systemctl start mysqld
 *     - # systemctl enable mysqld
 *     - (+ 원격접속 허용....root계정 모든 IP 허용)
 *     - # cd /usr/bin //--불필요....
 *     - # mysql
 *     - mysql> SELECT Host,User,Password FROM mysql.user;
 *     - mysql> INSERT INTO mysql.user (host, user, password, ssl_cipher, x509_issuer, x509_subject) VALUES ('%','root',password(''),'','','');
 *     - mysql> GRANT ALL PRIVILEGES ON *.* TO 'root'@'%';
 *     - mysql> FLUSH PRIVILEGES;
 *   2.2 run : Run As > Scala Application
 *   2.2 # Netcat 콘솔에 데이터 입력....	//--Netcat 데이터 송신....=> ApacheAccessLog 메시지 송신....
 *   
 */
object LogAnalyzerStreamingSQL {
  val WINDOW_LENGTH = new Duration(20 * 1000)
  val SLIDE_INTERVAL = new Duration(5 * 1000)

  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf().setAppName("Log Analyzer Streaming SQL").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sc, SLIDE_INTERVAL)

    val spark = SparkSession.builder
                                   .config(sc.getConf)
                                   .config("spark.sql.warehouse.dir", "file:///C:/Scala_IDE_for_Eclipse/eclipse/workspace/Spark2.0.2_Edu_Lab/spark-warehouse")
                                   .getOrCreate()
    import spark.implicits._    
    
    val subdf = spark.read.format("jdbc").option("url", "jdbc:mysql://CentOS7-14:3306/mysql").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(SELECT * FROM projects WHERE id > 3) as subprojects").option("user", "root").option("password", "").load().cache
    subdf.show()
    subdf.createOrReplaceTempView("projects")
    
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
        accessLogs.toDF().createOrReplaceTempView("logs")

        //--1. Calculate statistics based on the content size.
        val contentSizeStats = spark
          .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs")
          .first()
        println("1. Content Size Avg: %s, Min: %s, Max: %s".format(
          contentSizeStats.getLong(0) / contentSizeStats.getLong(1),
          contentSizeStats(2),
          contentSizeStats(3)))

        //--2. Compute Response Code to Count.
        val responseCodeToCount = spark
          .sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode")
          .map(row => (row.getInt(0), row.getLong(1)))
          .take(1000)
        println(s"""2. Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

        //--3. Any IPAddress that has accessed the server more than 10 times.
        val ipAddresses = spark
          .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10")
          .map(row => row.getString(0))
          .take(100)
        println(s"""3. IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")
        
        //--4. Top Endpoints.
        val topEndpoints = spark
          //.sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
          .sql("SELECT a.endpoint, COUNT(a.*) AS total FROM logs a JOIN projects b ON a.endpoint = b.website GROUP BY a.endpoint ORDER BY total DESC LIMIT 10")
          .map(row => (row.getString(0), row.getLong(1)))
          .collect()
        println(s"""4. Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
