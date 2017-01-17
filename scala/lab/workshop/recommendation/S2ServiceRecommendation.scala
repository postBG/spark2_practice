package lab.workshop.recommendation

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.mllib.evaluation.RegressionMetrics

import org.jblas.DoubleMatrix

import scalax.file.Path

/**
 * 사용자 기반 추천(User-based) vs. 제품 기반 추천(Item-based)....
 * 1. 특정 사용자에게 추천할 만한 아티스트....
 * 2. 특정 아티스트에게 추천할 만한 사용자....
 * 3. 모든 사람들이 좋아할 만한 아티스트....
 * 4. 특정 아티스트와 유사한 아티스트....(제품 기반 추천) => item-to-item similarities
 */
object S2ServiceRecommendation {
  def main(args: Array[String]): Unit = {
    
    val userID = 1000221
    val productID = 1000573
    val num = 5
    
    println(s">>>> #1. Recommend [ ${num} ] Products for User [ ${userID} ]____(product,prediction).......................................................................................")
    Recommender.recommendProducts(userID, num).foreach(println)
    
    println(s">>>> #2. Recommend [ ${num} ] Users for Product [ ${productID} ]____(user,prediction).......................................................................................")
    Recommender.recommendUsers(productID, num).foreach(println)
    
    println(s">>>> #3. Recommend [ ${num} ] Most Popular Products for All Users____(product,prediction).........................................................................")
    Recommender.predictMostListened(num).foreach(println)
    
    println(s">>>> #4. Recommend [ ${num} ] Products for Product [ ${productID} ]____(product,prediction)........................................................................")
    Recommender.recommendProductsForProduct(productID, num).foreach(println)
    
  }
}

object Recommender {
  
  //--#0. SparkContext 생성....
  lab.common.config.Config.setHadoopHOME
  
  val conf = new SparkConf().setAppName("Recommender").setMaster("local[*]")
  conf.set("spark.sql.warehouse.dir", "spark-warehouse")
  val sc = new SparkContext(conf)
  
  
  //--#1. 저장된 Best 모델을 로드....(저장 위치 : "lab.workshop.recommendation/recommendation-model")
  val path = "lab.workshop.recommendation/recommendation-model"
  val model = MatrixFactorizationModel.load(sc, path)
  
  
  //--#2. 아티스트별로 모든 사람들이 재생한 횟수를 더하여 내림차순 정렬 후 캐시....=> 모든 사람들이 좋아할 만한 아티스트 구하기에 사용하기 위해....
  val base = "src/main/resources/profiledata_06-May-2005/"
  
  val rawUserArtistData = sc.textFile(base + "user_artist_data.txt")
  //val rawArtistData = sc.textFile(base + "artist_data.txt")
  val rawArtistAlias = sc.textFile(base + "artist_alias.txt")
  
  //--데이터가 큰 변수를 각 task에서 참조하니까 브로드캐스트로 변수값을 한번 넘겨 재사용하도록 구현....
  val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))
  
  //--재생 정보에 포함된 아티스트 알리아스의 아이디를 원래 아티스트 아이디로 변경하여 Rating 생성....
  //val allData = buildRatings(rawUserArtistData, bArtistAlias)
  val allData = buildRatings(rawUserArtistData.randomSplit(Array(0.0001, 0.9999), 11L)(0), bArtistAlias)  //--원본의 0.01%에 해당하는 데이터를 전체 데이터로 간주하고 연산.... => 컴퓨터 리소스 부족 및 실습 소요시간 단축 위해....
  println(">>>> allData.count : " + allData.count())
  println(">>>> allData.first : " + allData.first())  
  
  //val bListenCount = allData.map(r => (r.product, r.rating)).reduceByKey(_ + _)
  val bListenCount = allData.map(r => (r.product, r.rating)).reduceByKey(_ + _).sortBy(_._2, false)
  bListenCount.cache
    
 
  
  
  //--#3. 특정 사용자에게 추천할 만한 아티스트....
  def recommendProducts(user: Int, num: Int) = {
    val recommendations = model.recommendProducts(user, num)
    recommendations.map(rating => (rating.product, rating.rating))
  }
  
  //--#4. 특정 아티스트에게 추천할 만한 사용자....
  def recommendUsers(product: Int, num: Int) = {
    val recommendations = model.recommendUsers(product, num)
    recommendations.map(rating => (rating.user, rating.rating))
  }
  
  //--#5. 모든 사람들이 좋아할 만한 아티스트....
  def predictMostListened(num: Int) = {
    //bListenCount.top(num)(Ordering.by[(Int, Double), Double] { case (product, ratingSum) => ratingSum })
    bListenCount.take(num)
  }
  
  //--#6. 특정 아티스트와 유사한 아티스트....(제품 기반 추천) => item-to-item similarities
  def recommendProductsForProduct(product: Int, num: Int) = {
    val itemFactor = model.productFeatures.lookup(product).head 
    val itemVector = new DoubleMatrix(itemFactor)
    
    val sims = model.productFeatures.map{ case (id, factor) => 
    	val factorVector = new DoubleMatrix(factor)
    	val sim = cosineSimilarity(factorVector, itemVector)
    	(id, sim)
    }
    
    val sortedSimsPlusOne = sims.top(num + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    val sortedSimsExceptSelf = sortedSimsPlusOne.slice(1, sortedSimsPlusOne.length)
    sortedSimsExceptSelf
  }
  
  
  
  //--ArtistAlias 데이터 구성....
  private def buildArtistAlias(rawArtistAlias: RDD[String]): Map[Int,Int] =
    rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()
    
  //--Rating 데이터 구성....
  private def buildRatings(
      rawUserArtistData: RDD[String],
      bArtistAlias: Broadcast[Map[Int,Int]]) = {
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      //--BadID를 GoodID로 변경....
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }
  } 
    
  //--코사인 유사성(Cosine Similarity) 메소드 구현....
  //--두 벡터 간 내적을 계산하고 각 벡터의 기준(혹은 길이)을 곱한 분모로 결과를 나눔....
  //--코사인 유사성은 -1과 1 사이의 값으로 측정.....
  //--1은 완전히 유사한 함. 0은 독립적(즉, 유사성 없음). -1은 유사하지 않을 뿐 아니라 완전히 다름.
  private def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
  	vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
}