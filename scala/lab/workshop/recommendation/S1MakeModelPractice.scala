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
object S1MakeModelPractice {
  def main(args: Array[String]): Unit = {
    
    //--#0. SparkContext 생성....
    
    //--#1. 학습 데이터 준비.... 원본 데이터 크기가 너무 크므로 0.0001 비율만 사용하여 이에 대해서 다시 Train Data(9) : Validataion Data(1) 로 구분하여 사용....
    
    //--#2. 하이퍼파라미터 조합별 평가, 최적 파리미터 추출 및 베스트모델 학습....... 조합 8개(2 * 2 * 2) => ( rank   <- Array(10,  50); lambda <- Array(1.0, 0.0001); alpha  <- Array(1.0, 40.0) ).... 평가는 Root Mean Squared Error (RMSE) 사용....
    
    //--#3. 베스트모델 저장....(저장 위치 : "lab.workshop.recommendation/recommendation-model")
 
  }  
}