package lab.core.customapi

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class Kikang(rdd:RDD[SalesRecord]) {

  def totalSalesKikang = rdd.map(_.itemValue).sum  //--Double 리턴....
  
  def discountRateKikang(discountRate:Double) = new DiscountRateRDD(rdd, discountRate)  //--DiscountRateRDD 리턴.... => 커스텀 RDD 리턴....
  
  def discountPercentageKikang(discountPercentage:Double) = new DiscountPercentageRDD(rdd, discountPercentage)  //--DiscountPercentageRDD 리턴.... => 커스텀 RDD 리턴....

}

class Keru(rdd: RDD[String]) {
  
  def reduceKeru = rdd.reduce(_ + "+" + _)  //--String 리턴....
  
  def mapKeru = rdd.map { x => x + "_Keru" }  //--RDD 리턴....
}


object CustomFunctions {
  
  implicit def kikang(rdd: RDD[SalesRecord]) = new Kikang(rdd)
  
  implicit def keru(rdd: RDD[String]) = new Keru(rdd)
}