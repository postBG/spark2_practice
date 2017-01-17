package lab.ml.twitter.classifier.language

import java.io.File
import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scalax.file.Path

/**
 * Collect at least the specified number of tweets into json text files....
 * 
 * 1. IDE에서 local 실행....
 *   1.1 run : Run As > Scala Application
 * 
 */
object Collect {
  private var numTweetsCollected = 0L
  private var partNum = 0
  private var gson = new Gson()

  def main(args: Array[String]) {
    
    //--윈도우 환경을 위한 winutils.exe (Hadoop binaries) 설정....
    lab.common.config.Config.setHadoopHOME
    
    //--트윗 수집 디렉토리 경로, 수집 트윗 개수, 스트리밍 배치 인터벌, 스트림 RDD 파티션 개수 설정....
    val Array(outputDirectory, numTweetsToCollect,  intervalSecs, partitionsEachInterval) 
      = Array(this.getClass.getPackage.getName, "1000", "5", "1")
    
    //--기존 트윗 수집 디렉토리 삭제....
    try(Path(outputDirectory).deleteRecursively(continueOnFailure = false)) catch {case e: Exception =>}
    
    //--스트리밍 컨텍스트 생성....
    println(">>>> Initializing Streaming Spark Context....")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    conf.set("spark.sql.warehouse.dir", "spark-warehouse")  //--윈도우 환경을 위한 설정.... (for Spark SQL, Hive)
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(intervalSecs.toLong))
    
    
    //--트위터 스트림 생성....
    println(">>>> Initializing Twitter stream....")
    val tweetInputDStream = TwitterUtils.createStream(ssc, Utils.getAuth)
    
    //--트위터 스트림 내용 출력....=> 디버깅용....
    tweetInputDStream.foreachRDD((rdd, time) => 
      rdd.foreach { status => 
        println(s">>>> status (@ ${time.milliseconds}) : ${status}")
        println(s">>>> status.getText (@ ${time.milliseconds}) : ${status.getText}")
      }
    )
    
    //--트위터 Status 객체를 JSON 스트링으로 트랜스폼.... => 향후 Spark SQL로 분석하기 위해 로딩 및 쿼리가 용이한 JSON 형태로 변형....
    val tweetStream = tweetInputDStream.map(gson.toJson(_))
    
    //--json 스트링을 파일로 저장....  
    tweetStream.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {  //--해당 스트림 배치 인터벌에 데이터가 있을 경우....
        //--파티션 별로 파일이 생성됨....=> 적절한 파일 개수를 위해 파티션 조정(리파티션)....
        val outputRDD = rdd.repartition(partitionsEachInterval.toInt)
        //--RDD 내용을 파일로 저장....
        outputRDD.saveAsTextFile(outputDirectory + "/tweets_" + time.milliseconds.toString)
        //--수집된 트윗 개수 누적....
        numTweetsCollected += count
        //--수집된 트윗 개수를 수집하고자 하는 트윗 개수 목표치와 비교하여 요건 만족시 트윗 수집 종료(스트리밍 앱 종료)....  
        if (numTweetsCollected >= numTweetsToCollect.toLong) {
          println(s">>>> numTweetsCollected : ${numTweetsCollected}")
          System.exit(0)
        }
      }
    })
    
    //--스트리밍 컨텍스트 시작 및 종료대기....
    ssc.start()
    ssc.awaitTermination()
  }
}
