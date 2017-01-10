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




//--[Stream #01]....
val tweets = TwitterUtils.createStream(ssc, None)


val hashTags = tweets.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
case class Tweet(topic:String, count:Long)
val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                 .map{case (topic, count) => (count, topic)}
                 .transform(_.sortByKey(false))
                 .map{case (count, topic) => Tweet(topic, count)}

val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
                 .map{case (topic, count) => (count, topic)}
                 .transform(_.sortByKey(false))
                 .map{case (count, topic) => Tweet(topic, count)}


//--Print popular hashtags
topCounts60.foreachRDD(rdd => {
  rdd.toDF().createOrReplaceTempView("topCounts60Tab")
})

topCounts10.foreachRDD(rdd => {
  rdd.toDF().createOrReplaceTempView("topCounts10Tab")
})



topCounts60.print
topCounts10.print




//--[Stream #02]....
val tweets2 = TwitterUtils.createStream(ssc, None).window(Seconds(120))


case class Tweet2(createdAt:Long, text:String)
tweets2.map(status=>
  Tweet2(status.getCreatedAt().getTime()/1000, status.getText())
).foreachRDD(rdd=>
  rdd.toDF().createOrReplaceTempView("tweets2")
)

tweets2.print




ssc.start()
//ssc.awaitTermination()
==========================================================================================
 
 
========================================================================================== 
%sql
select *
from topCounts60Tab
limit 5
==========================================================================================
 
========================================================================================== 
%spark.sql
select *
from topCounts10Tab
limit 5 
==========================================================================================

========================================================================================== 
%spark.sql
select *
from tweets2
where text like '%한국%'
limit 5
==========================================================================================


 * 
 */
object Zeppelin02 {
  
}