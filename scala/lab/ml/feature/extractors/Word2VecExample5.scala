package lab.ml.feature.extractors

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

import com.twitter.penguin.korean.TwitterKoreanProcessor
import com.twitter.penguin.korean.phrase_extractor.KoreanPhraseExtractor.KoreanPhrase
import com.twitter.penguin.korean.tokenizer.KoreanTokenizer.KoreanToken

import scalax.file.Path

/**
 * 1. IDE에서 local 실행....
 *   1.1 run : Run As > Scala Application
 *   
 */
object Word2VecExample5 {

  def main(args: Array[String]): Unit = {
    
    try(Path (this.getClass.getName).deleteRecursively(continueOnFailure = false)) catch {case e: Exception =>}

    lab.common.config.Config.setHadoopHOME
    
    val conf = new SparkConf().setAppName("Word2VecExample").setMaster("local[*]")
    conf.set("spark.sql.warehouse.dir", "spark-warehouse")
    val sc = new SparkContext(conf)

    val input = sc.textFile("src/main/resources/news_*.txt")
                      .map(text => TwitterKoreanProcessor.normalize(text))
                      .map(normalized => TwitterKoreanProcessor.tokenize(normalized))
                      .map(tokens => TwitterKoreanProcessor.extractPhrases(tokens, filterSpam = true, enableHashtags = true))
                      .map{phrases =>
                        var list: List[String] = List[String]()  
                        phrases.foreach { x => list = x.text :: list }
                        //println(list)
                        list
                      }

    val word2vec = new Word2Vec()

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("최씨", 5)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    //--Save and load model
    model.save(sc, this.getClass.getName)
    val sameModel = Word2VecModel.load(sc, this.getClass.getName)
    
    sameModel.getVectors.foreach{row => 
      sameModel.findSynonyms(row._1, 5).foreach{re => 
        println(s"${row._1} => ${re._1} ${re._2}")
      }
    }
    sc.stop()
  }
}