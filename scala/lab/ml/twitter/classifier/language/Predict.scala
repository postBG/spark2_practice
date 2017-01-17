package lab.ml.twitter.classifier.language

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Pulls live tweets and filters them for tweets in the chosen cluster....
 * 
 * 1. IDE에서 local 실행....
 *   1.1 run : Run As > Scala Application
 * 
 */
object Predict {
  def main(args: Array[String]) {
    
    //--윈도우 환경을 위한 winutils.exe (Hadoop binaries) 설정....
    lab.common.config.Config.setHadoopHOME
    
    //--모델 저장 디렉토리 경로(머신러닝), 필터링 대상 클러스터 번호(머신러닝) 설정....
    val Array(modelFile, clusterNumber) 
      = Array(this.getClass.getPackage.getName + "/model", 30)
    
    //--스트리밍 컨텍스트 생성....
    println(">>>> Initializing Streaming Spark Context....")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    
    //--트위터 스트림 생성....
    println(">>>> Initializing Twitter stream....")
    val tweets = TwitterUtils.createStream(ssc, Utils.getAuth)
    val tweetsText = tweets.map(_.getText)
    
    //--모델 생성....
    println(">>>> Initalizaing the the KMeans model....")
    val model = new KMeansModel(ssc.sparkContext.objectFile[Vector](modelFile.toString).collect())
    
    //--필터링 대상 클러스터 번호로 트윗 텍스트 필터링....
    val filteredTweets = tweetsText.filter(text => model.predict(Utils.featurize(text)) == clusterNumber)
    //--필터링 된 트윗 텍스트 출력....
    filteredTweets.print()

    //--Start the streaming computation....
    println(">>>> Initialization complete....")
    println(s">>>> clusterNumber : ${clusterNumber}")
    
    //--스트리밍 컨텍스트 시작 및 종료대기....
    ssc.start()
    ssc.awaitTermination()
  }
}
