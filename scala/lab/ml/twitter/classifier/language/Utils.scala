package lab.ml.twitter.classifier.language

import org.apache.commons.cli.{Options, ParseException, PosixParser}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.HashingTF
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object Utils {

 def getAuth = {
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = Array("45J8nvQkcxHBTaKEj0GjdFFE4", "tnRTcaPcZkFnLSudp9p7cGh64VfDD8cY4ZknB4aVfYpBnoABuA", "959497934-qVUGKFDVcbvdR0WlCY9bJpF2GWPavovH0wmhrXSG", "fOIapjdBbFwn1W0oH0JvIuiL8zNtjueqo2OGSbnJi1e1U")
    val builder: ConfigurationBuilder = new ConfigurationBuilder()
    builder.setOAuthConsumerKey(consumerKey);
    builder.setOAuthConsumerSecret(consumerSecret);
    builder.setOAuthAccessToken(accessToken);
    builder.setOAuthAccessTokenSecret(accessTokenSecret);
    Some(new OAuthAuthorization(builder.build()))
  }

  /**
   * Create feature vectors by turning each tweet into bigrams of characters (an n-gram model)
   * and then hashing those to a length-1000 feature vector that we can pass to MLlib.
   * This is a common way to decrease the number of features in a model while still
   * getting excellent accuracy (otherwise every pair of Unicode characters would
   * potentially be a feature).
   */
  val numFeatures = 1000
  val tf = new HashingTF(numFeatures)
  
  def featurize(s: String): Vector = {
    tf.transform(s.sliding(2).toSeq)
  }
}
