package lab.core.customapi

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import CustomFunctions._


/**
 * 1. 커스텀 메소드, 커스텀 RDD 생성....
 *  1.1 커스텀 메소드 : itemValue의 총합(sum)을 리턴하는 메소드 추가....=> RDD에 totalSalesKikang 메소드 추가....
 *  1.2 커스텀 메소드 : 할인(discount)이 적용된 itemValue로 구성된 커스텀 RDD를 리턴 
 * 
 */
object ExtendExample {

  def main(args: Array[String]) {
    
    lab.common.config.Config.setHadoopHOME
    
    val sc = new SparkContext("local[2]", "CustomAPIExample")
    val dataRDD = sc.textFile("src/main/resources/sales.csv")
    val salesRecordRDD = dataRDD.map(row => {
      val colValues = row.split(",")
      new SalesRecord(colValues(0), colValues(1), colValues(2), colValues(3).toDouble)
    })
    
    //--itemValue List.... => itemValue 값 나열....
    println(">>>> 01. salesRecordRDD.map(_.itemValue).collect().toList : " + salesRecordRDD.map(_.itemValue).collect().toList)
    
    println(" ")
    
    
    //--map().sum.... => itemValue 값의 합(sum)....
    println(">>>> 02-1. salesRecordRDD.map(_.itemValue).sum: " + salesRecordRDD.map(_.itemValue).sum)
    
    //--totalSalesKikang.... => itemValue 값의 합(sum)을 리턴해 주는 커스텀 메소드(totalSalesKikang)....
    println(">>>> 02-2. salesRecordRDD.totalSalesKikang: " + salesRecordRDD.totalSalesKikang)

    println(" ")
    
    
    //--discountRateKikang.... => 할인(discount)이 적용된 itemValue로 구성된 커스텀 RDD를 리턴.... => 기존 itemValue의 0.1 비율로 할인적용....
    val discountRateKikangRDD = salesRecordRDD.discountRateKikang(0.1) 
    println(">>>> 03-1. discountRateKikangRDD.collect().toList : " + discountRateKikangRDD.collect().toList)
    
    //--discountPercentageKikang.... => 할인(discount)이 적용된 itemValue로 구성된 커스텀 RDD를 리턴.... => 기존 itemValue의 10% 비율로 할인적용....
    val discountPercentageKikangRDD = salesRecordRDD.discountPercentageKikang(10)
    println(">>>> 03-2. discountPercentageKikangRDD.collect().toList: " + discountPercentageKikangRDD.collect().toList)
    
    println(" ")
    
    
    //--reduce(_ + "+" + _).... => 모든 고객아이디(customerId) "+" 로 연결하여 리턴....
    println(""">>>> 04-1. salesRecordRDD.map(_.customerId).reduce(_ + "+" + _) : """ + salesRecordRDD.map(_.customerId).reduce(_ + "+" + _) ) 
    
    //--reduceKeru.... => 모든 고객아이디(customerId) "+" 로 연결하여 리턴해 주는 커스텀 메소드(reduceKeru)....
    println(">>>> 04-2. salesRecordRDD.map(_.customerId).reduceKeru : " + salesRecordRDD.map(_.customerId).reduceKeru)
    
    println(" ")
    
    
    //--map { x => x + "_Keru" }.... => 아이템아이디(itemId)에 접미사(suffix) "_Keru"를 붙이자....
    val mapRDD = salesRecordRDD.map { x => x.itemId }.map { x => x + "_Keru" }
    println(">>>> 05-1. mapRDD.collect().toList : " + mapRDD.collect().toList)
    
    //--mapKeru....=> 아이템아이디(itemId)에 접미사(suffix) "_Keru"를 붙이는 커스텀 메소드(mapKeru) 사용....
    val mapKeruRDD = salesRecordRDD.map { x => x.itemId }.mapKeru
    println(">>>> 05-2. mapKeruRDD.collect().toList : " + mapKeruRDD.collect().toList)
  }

}