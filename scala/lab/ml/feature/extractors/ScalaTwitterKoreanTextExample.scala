package lab.ml.feature.extractors

import com.twitter.penguin.korean.TwitterKoreanProcessor
import com.twitter.penguin.korean.phrase_extractor.KoreanPhraseExtractor.KoreanPhrase
import com.twitter.penguin.korean.tokenizer.KoreanTokenizer.KoreanToken

/**
 * 1. IDE에서 local 실행....
 *   1.1 run : Run As > Scala Application
 *   
 */
object ScalaTwitterKoreanTextExample {
  def main(args: Array[String]) {
    val text = "한국어를 처리하는 예시입니닼ㅋㅋㅋㅋㅋ #한국어"
    println(">>>> 원문 : " + text)
    
    //--Normalize
    val normalized: CharSequence = TwitterKoreanProcessor.normalize(text)
    println(">>>> Normalize : " + normalized)
    //--한국어를 처리하는 예시입니다ㅋㅋ #한국어

    //--Tokenize
    val tokens: Seq[KoreanToken] = TwitterKoreanProcessor.tokenize(normalized)
    println(">>>> Tokenize : " + tokens)
    //--List(한국어(Noun: 0, 3), 를(Josa: 3, 1),  (Space: 4, 1), 처리(Noun: 5, 2), 하는(Verb: 7, 2),  (Space: 9, 1), 예시(Noun: 10, 2), 입니(Adjective: 12, 2), 다(Eomi: 14, 1), ㅋㅋ(KoreanParticle: 15, 2),  (Space: 17, 1), #한국어(Hashtag: 18, 4))

    //--Stemming
    val stemmed: Seq[KoreanToken] = TwitterKoreanProcessor.stem(tokens)
    println(">>>> Stemming : " + stemmed)
    //--List(한국어(Noun: 0, 3), 를(Josa: 3, 1),  (Space: 4, 1), 처리(Noun: 5, 2), 하다(Verb: 7, 2),  (Space: 9, 1), 예시(Noun: 10, 2), 이다(Adjective: 12, 3), ㅋㅋ(KoreanParticle: 15, 2),  (Space: 17, 1), #한국어(Hashtag: 18, 4))

    //--Phrase extraction
    val phrases: Seq[KoreanPhrase] = TwitterKoreanProcessor.extractPhrases(tokens, filterSpam = true, enableHashtags = true)
    println(">>>> Phrase extraction : " + phrases)
    //--List(한국어(Noun: 0, 3), 처리(Noun: 5, 2), 처리하는 예시(Noun: 5, 7), 예시(Noun: 10, 2), #한국어(Hashtag: 18, 4))
    
    phrases.foreach { x => println(x.text) }
  }
}