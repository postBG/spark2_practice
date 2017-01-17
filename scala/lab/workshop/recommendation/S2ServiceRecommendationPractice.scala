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
object S2ServiceRecommendationPractice {
  def main(args: Array[String]): Unit = {
    
    val userID = 1000221
    val productID = 1000573    
    val num = 5
    
    /*
    println(s">>>> #1. Recommend [ ${num} ] Products for User [ ${userID} ]____(product,prediction).......................................................................................")
    Recommender.recommendProducts(userID, num).foreach(println)
    
    println(s">>>> #2. Recommend [ ${num} ] Users for Product [ ${productID} ]____(user,prediction).......................................................................................")
    Recommender.recommendUsers(productID, num).foreach(println)
    
    println(s">>>> #3. Recommend [ ${num} ] Most Popular Products for All Users____(product,prediction).........................................................................")
    Recommender.predictMostListened(num).foreach(println)
    
    println(s">>>> #4. Recommend [ ${num} ] Products for Product [ ${productID} ]____(product,prediction)........................................................................")
    Recommender.recommendProductsForProduct(productID, num).foreach(println)
    */
  }
}

object RecommenderPractice {
  
  //--#0. SparkContext 생성....
  
  
  //--#1. 저장된 Best 모델을 로드....(저장 위치 : "lab.workshop.recommendation/recommendation-model")
  
  
  //--#2. 아티스트별로 모든 사람들이 재생한 횟수를 더하여 내림차순 정렬 후 캐시....=> 모든 사람들이 좋아할 만한 아티스트 구하기에 사용하기 위해....
  
  
  //--#3. 특정 사용자에게 추천할 만한 아티스트....
  def recommendProducts(user: Int, num: Int) = {
    
  }
  
  //--#4. 특정 아티스트에게 추천할 만한 사용자....
  def recommendUsers(product: Int, num: Int) = {
    
  }
  
  //--#5. 모든 사람들이 좋아할 만한 아티스트....
  def predictMostListened(num: Int) = {
    
  }
  
  //--#6. 특정 아티스트와 유사한 아티스트....(제품 기반 추천) => item-to-item similarities
  def recommendProductsForProduct(product: Int, num: Int) = {
    
  }
}