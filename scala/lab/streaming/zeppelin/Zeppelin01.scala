package lab.streaming.zeppelin

/**
 * 
 5. Twitter 스트리밍 코드 + SQL 작성 및 실행....
   

==========================================================================================
%spark
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf


val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = Array("45J8nvQkcxHBTaKEj0GjdFFE4", "tnRTcaPcZkFnLSudp9p7cGh64VfDD8cY4ZknB4aVfYpBnoABuA", "959497934-qVUGKFDVcbvdR0WlCY9bJpF2GWPavovH0wmhrXSG", "fOIapjdBbFwn1W0oH0JvIuiL8zNtjueqo2OGSbnJi1e1U")


System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
System.setProperty("twitter4j.oauth.accessToken", accessToken)
System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

val ssc = new StreamingContext(sc, Seconds(2))


val stream = TwitterUtils.createStream(ssc, None)

val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                 .map{case (topic, count) => (count, topic)}
                 .transform(_.sortByKey(false))

val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
                 .map{case (topic, count) => (count, topic)}
                 .transform(_.sortByKey(false))


//--Print popular hashtags
topCounts60.foreachRDD(rdd => {
  val topList = rdd.take(10)
  println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
  topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  
  rdd.toDF().createOrReplaceTempView("topCounts60Tab")
})

topCounts10.foreachRDD(rdd => {
  val topList = rdd.take(10)
  println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
  topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  
  rdd.toDF().createOrReplaceTempView("topCounts10Tab")
})



ssc.start()
//ssc.awaitTermination()    
==========================================================================================
 
 
========================================================================================== 
%sql
select _2 as hashtag, _1 as count_win60s
from topCounts60Tab
limit 5
==========================================================================================
 
========================================================================================== 
%spark.sql
select _2 as hashtag, _1 as count_win10s
from topCounts10Tab
limit 5 
==========================================================================================


 * 
 */
object Zeppelin01 {
  
}