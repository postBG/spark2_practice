package lab.streaming.twitter

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 * 
 * 0. 트위터 계정 + 트위터 App (with Consumer Key and Access Token) 준비
 *  0.1 트위터 App (https://apps.twitter.com/)
 *  
 * 1. IDE에서 local 실행....
 *   1.1 run : Run As > Scala Application
 *   
 */
object TwitterPopularTags {
  def main(args: Array[String]) {
    
    lab.common.config.Config.setHadoopHOME
    
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = Array("45J8nvQkcxHBTaKEj0GjdFFE4", "tnRTcaPcZkFnLSudp9p7cGh64VfDD8cY4ZknB4aVfYpBnoABuA", "959497934-qVUGKFDVcbvdR0WlCY9bJpF2GWPavovH0wmhrXSG", "fOIapjdBbFwn1W0oH0JvIuiL8zNtjueqo2OGSbnJi1e1U")
    val filters = args.takeRight(args.length - 0)

    //--Set the system properties so that Twitter4j library used by twitter stream
    //--can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TwitterPopularTags")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

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
      println("\n>>>> [last 60 seconds] Popular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("\t>>>> [last 60 seconds]  %s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\n>>>> [last 10 seconds] Popular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("\t>>>> [last 10 seconds]  %s (%s tweets)".format(tag, count))}
    })

    ssc.start()
    ssc.awaitTermination()
  }
}