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
 * 0. 모델 생성 + 파일 저장 코드 작성 실습....
 *   
 * 1. IDE에서 local 실행....
 *   1.1 run config : Run > Run Configurations > Arguments > VM arguments => -Xms4096m -Xmx4096m (실습환경에서 허용하는 범위 내에서 가능한 많은 메모리 할당....)
 *   1.2 run : Run As > Scala Application
 */
object S1MakeModel {
  def main(args: Array[String]): Unit = {
    
    //--#0. SparkContext 생성....
    val start = System.currentTimeMillis()
    
    lab.common.config.Config.setHadoopHOME
    
    val conf = new SparkConf().setAppName("S1MakeModel").setMaster("local[*]")
    conf.set("spark.sql.warehouse.dir", "spark-warehouse")
    val sc = new SparkContext(conf)
    
    
    //--#1. 학습 데이터 준비.... 원본 데이터 크기가 너무 크므로 0.0001 비율만 사용하여 이에 대해서 다시 Train Data(9) : Validataion Data(1) 로 구분하여 사용....
    val base = "src/main/resources/profiledata_06-May-2005/"
    
    val rawUserArtistData = sc.textFile(base + "user_artist_data.txt")
    val rawArtistData = sc.textFile(base + "artist_data.txt")
    val rawArtistAlias = sc.textFile(base + "artist_alias.txt")
        
    //--데이터가 큰 변수를 각 task에서 참조하니까 브로드캐스트로 변수값을 한번 넘겨 재사용하도록 구현....
    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))
    
    //--재생 정보에 포함된 아티스트 알리아스의 아이디를 원래 아티스트 아이디로 변경하여 Rating 생성....  + 캐시....
    //val allData = buildRatings(rawUserArtistData, bArtistAlias).cache()
    val allData = buildRatings(rawUserArtistData.randomSplit(Array(0.0001, 0.9999), 11L)(0), bArtistAlias).cache()  //--원본의 0.01%에 해당하는 데이터를 전체 데이터로 간주하고 연산.... => 컴퓨터 리소스 부족 및 실습 소요시간 단축 위해....
    println(">>>> allData.count : " + allData.count())
    println(">>>> allData.first : " + allData.first())
    
    //--훈련데이터, 테스트 데이터 분리.... + 캐시....
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    cvData.cache()
    
    
    //--#2. 하이퍼파라미터 조합별 평가, 최적 파리미터 추출 및 베스트모델 학습....... 조합 8개(2 * 2 * 2) => ( rank   <- Array(10,  50); lambda <- Array(1.0, 0.0001); alpha  <- Array(1.0, 40.0) ).... 평가는 Root Mean Squared Error (RMSE) 사용....
    val evaluations =      
      //--조합이 8개(2 * 2 * 2)일 경우....
      for (rank   <- Array(10,  50);
           lambda <- Array(1.0, 0.0001);
           alpha  <- Array(1.0, 40.0))      
      /*
      //--조합 1개일 경우.... => 빠른 테스트를 위해 조합 1개로 테스트....
      for (rank   <- Array(2);
           lambda <- Array(1.0);
           alpha  <- Array(40.0))
      */
        yield {
        val model = ALS.trainImplicit(trainData, rank, 2, lambda, alpha)
        val rmse = rootMeanSquaredError(cvData, model.predict)        
        unpersist(model)
        ((rank, lambda, alpha), rmse)
      }
    
    //--RMSE 값으로 '오름차순' 정렬....
    val evaluationsResult = evaluations.sortBy(_._2)
    trainData.unpersist()
    cvData.unpersist()
        
    //--Best 하이퍼파라미터 추출....
    val (bestRank, bestLambda, bestAlpha) = evaluationsResult.head._1
    //--Best 하이퍼파라미터 출력....
    println(s">>>> Best hyperparameter => Rank: ${bestRank}, Lambda: ${bestLambda}, Alpha: ${bestAlpha}")
    
    //--Best 하이퍼파라미터를 이용하여 전체 데이터로 모델을 학습....
    val (blocks, seed) = (12, 11L)
    val bestModel = ALS.trainImplicit(allData, bestRank, 2, bestLambda, blocks, bestAlpha, seed)
    allData.unpersist()
    
    
    //--#3. 베스트모델 저장....(저장 위치 : "lab.workshop.recommendation/recommendation-model")
    //--Best 모델 저장....
    val path = "lab.workshop.recommendation/recommendation-model"
    try(Path (path).deleteRecursively(continueOnFailure = false)) catch {case e: Exception =>}
    bestModel.save(sc, path)
    unpersist(bestModel)
    
    println(">>>> Lap time : " + (System.currentTimeMillis() - start)) 
  }
  
  //--ArtistAlias 데이터 구성....
  def buildArtistAlias(rawArtistAlias: RDD[String]): Map[Int,Int] =
    rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()
    
  //--Rating 데이터 구성....
  def buildRatings(
      rawUserArtistData: RDD[String],
      bArtistAlias: Broadcast[Map[Int,Int]]) = {
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      //--BadID를 GoodID로 변경....
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }
  } 
  
  //--Root Mean Squared Error (RMSE)....
  def rootMeanSquaredError(
      cvData: RDD[Rating],
      predictFunction: (RDD[(Int,Int)] => RDD[Rating])) = {
    //--Evaluate the model on rating data
    val usersProducts = cvData.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions = predictFunction(usersProducts).map { case Rating(user, product, prediction) =>
        ((user, product), prediction)
      }
    val predsAndRates = predictions.join(cvData.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    })
    val predictionAndLabels = predsAndRates.map { case ((user, product), (prediction, rate)) =>
      (prediction, rate)
    }
    //--Get the RMSE using regression metrics
    val regressionMetrics = new RegressionMetrics(predictionAndLabels)
    println(s">>>> RMSE = ${regressionMetrics.rootMeanSquaredError}")
    
    //--MSE
    println(s">>>> MSE = ${regressionMetrics.meanSquaredError}")
    
    //--R-squared
    println(s">>>> R-squared = ${regressionMetrics.r2}")
    
    regressionMetrics.rootMeanSquaredError
  }
  
  def unpersist(model: MatrixFactorizationModel): Unit = {
    //--At the moment, it's necessary to manually unpersist the RDDs inside the model
    //--when done with it in order to make sure they are promptly uncached
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }  
}