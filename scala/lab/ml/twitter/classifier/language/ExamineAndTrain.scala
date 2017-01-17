package lab.ml.twitter.classifier.language

import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.Vector

import scalax.file.Path

/**
 * Examine the collected tweets and trains a model based on them....
 * 
 * 1. IDE에서 local 실행....
 *   1.1 run : Run As > Scala Application
 * 
 */
object ExamineAndTrain {
  
  //--JSON 처리를 위한 관련 객체 생성....
  val jsonParser = new JsonParser()
  val gson = new GsonBuilder().setPrettyPrinting().create()

  def main(args: Array[String]) {
    
    //--윈도우 환경을 위한 winutils.exe (Hadoop binaries) 설정....
    lab.common.config.Config.setHadoopHOME
    
    //--트윗 수집 디렉토리 경로, 모델 저장 디렉토리 경로(머신러닝), 클러스터 개수(머신러닝), 반복 회수(머신러닝) 설정....
    val Array(tweetInput, outputModelDir, numClusters, numIterations) 
      = Array(this.getClass.getPackage.getName + "/tweets_*/part-*", this.getClass.getPackage.getName + "/model", "100", "10")
    
    //--모델 저장 디렉토리 삭제....
    try(Path(outputModelDir).deleteRecursively(continueOnFailure = false)) catch {case e: Exception =>}
    
    //--스파크 컨텍스트 생성....
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    conf.set("spark.sql.warehouse.dir", "spark-warehouse")  //--윈도우 환경을 위한 설정.... (for Spark SQL, Hive)
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .config(sc.getConf)
      .getOrCreate()

    //--For implicit conversions like converting RDDs to DataFrames....
    import spark.implicits._


    //--Pretty print some of the tweets....
    val tweets = sc.textFile(tweetInput)
    println(">>>> ----------- Sample JSON Tweets -----------")
    val take = tweets.take(5)
    println(s">>>> take.length : ${take.length} \n")
    for (tweet <- tweets.take(5)) {
      println(s">>>> Pretty print : ${gson.toJson(jsonParser.parse(tweet))}")
      println(s">>>> Raw print : ${tweet} \n")
    }
    println(s"\n")
    
    //--Spark SQL로 JSON 파일 읽은 후 캐시.... => 결과는 Dataset....
    val tweetTable = spark.read.json(tweetInput).cache()
    //-- 캐시된 Dataset으로 Temp View 생성....
    tweetTable.createOrReplaceTempView("tweetTable")
    
    //--Temp View 스키마 출력....
    println(">>>> ----------- Tweet Table Schema -----------")
    tweetTable.printSchema()
    println(s"\n")
    
    //--샘플 트윗 텍스트 출력....(10개)
    println(">>>> ----------- Sample Tweet Text -----------")
    spark.sql("SELECT text FROM tweetTable LIMIT 10").collect().foreach(println)
    println(s"\n")
    
    //--샘플 랭귀지, 이름, 트윗 텍스트 출력....(100개)
    println(">>>> ----------- Sample Lang, Name, Text -----------")
    spark.sql("SELECT user.lang, user.name, text FROM tweetTable LIMIT 100").collect().foreach(println)
    println(s"\n")
    
    //--랭귀지별 개수 집계 및 내림차순 정렬....(25개)
    println(">>>> ----------- Total Count by Languages Lang, Count(*) -----------")
    spark.sql("SELECT user.lang, COUNT(*) as cnt FROM tweetTable GROUP BY user.lang ORDER BY cnt DESC LIMIT 25").collect.foreach(println)
    println(s"\n")
    
    //--모델 학습 + 저장....
    println(">>>> ----------- Training the Model and Persist it -----------")
    val texts = spark.sql("SELECT text from tweetTable").map(_.toString)    
    val vectors = texts.map(Utils.featurize)(org.apache.spark.sql.Encoders.kryo[Vector]).rdd.cache()  //--Cache the vectors RDD since it will be used for all the KMeans iterations.
    vectors.setName("vectorsRDD")
    vectors.count()  //--Calls an action on the RDD to populate the vectors cache.
    val model = KMeans.train(vectors, numClusters.toInt, numIterations.toInt)    //--모델 학습....
    sc.makeRDD(model.clusterCenters, numClusters.toInt).saveAsObjectFile(outputModelDir)  //--모델 저장....
    
    //--모델 적용.... => 트윗 Text 클러스터링....
    val some_tweets = spark.sql("SELECT user.lang, text from tweetTable LIMIT 100").rdd
    println(">>>> ----------- Example Tweets from the Clusters -----------")
    for (i <- 0 until numClusters.toInt) {
      println(s"\n>>>> CLUSTER $i:")
      some_tweets.foreach { row =>
        if (model.predict(Utils.featurize(row.getString(1))) == i) {
          //println(s">>>> row.getString(0) : ${row.getString(0)}")
          //println(s">>>> row.getString(1) : ${row.getString(1)}")
          println(row)
        }
      }
    }
    println(s"\n")
  }
}
