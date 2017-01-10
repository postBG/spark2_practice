package lab.streaming.twitter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils


/**
 * Displays the most positive hash tags by joining the streaming Twitter data with a static RDD of
 * the AFINN word list (http://neuro.imm.dtu.dk/wiki/AFINN)
 * 
 * 0. 트위터 계정 + 트위터 App (with Consumer Key and Access Token) 준비
 *  0.1 트위터 App (https://apps.twitter.com/)
 *  
 * 1. IDE에서 local 실행....
 *   1.1 run : Run As > Scala Application
 *   
 */
object TwitterHashTagJoinSentiments {
  def main(args: Array[String]) {
    
    lab.common.config.Config.setHadoopHOME
    
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = Array("45J8nvQkcxHBTaKEj0GjdFFE4", "tnRTcaPcZkFnLSudp9p7cGh64VfDD8cY4ZknB4aVfYpBnoABuA", "959497934-qVUGKFDVcbvdR0WlCY9bJpF2GWPavovH0wmhrXSG", "fOIapjdBbFwn1W0oH0JvIuiL8zNtjueqo2OGSbnJi1e1U")
    val filters = args.takeRight(args.length - 0)

    //--Set the system properties so that Twitter4j library used by Twitter stream
    //--can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TwitterHashTagJoinSentiments")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    //--Read in the word-sentiment list and create a static RDD from it
    val wordSentimentFilePath = "spark-2.0.2-bin-hadoop2.7/data/streaming/AFINN-111.txt"
    val wordSentiments = ssc.sparkContext.textFile(wordSentimentFilePath).map { line =>
      val Array(word, happinessValue) = line.split("\t")
      (word, happinessValue.toInt)
    }.cache()

    //--Determine the hash tags with the highest sentiment values by joining the streaming RDD
    //--with the static RDD inside the transform() method and then multiplying
    //--the frequency of the hash tag by its sentiment value
    val happiest60 = hashTags.map(hashTag => (hashTag.tail, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(60))
      .transform{topicCount => wordSentiments.join(topicCount)}
      .map{case (topic, tuple) => (topic, tuple._1 * tuple._2)}
      .map{case (topic, happinessValue) => (happinessValue, topic)}
      .transform(_.sortByKey(false))

    val happiest10 = hashTags.map(hashTag => (hashTag.tail, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(10))
      .transform{topicCount => wordSentiments.join(topicCount)}
      .map{case (topic, tuple) => (topic, tuple._1 * tuple._2)}
      .map{case (topic, happinessValue) => (happinessValue, topic)}
      .transform(_.sortByKey(false))

    //--Print hash tags with the most positive sentiment values
    happiest60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\n>>>> [last 60 seconds] Happiest topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (happiness, tag) => println("\t>>>> [last 60 seconds]  %s (%s happiness) in last 60 seconds".format(tag, happiness))}
    })

    happiest10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\n>>>> [last 10 seconds] Happiest topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (happiness, tag) => println("\t>>>> [last 10 seconds]  %s (%s happiness) in last 10 seconds".format(tag, happiness))}
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
