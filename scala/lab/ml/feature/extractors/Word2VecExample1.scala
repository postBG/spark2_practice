package lab.ml.feature.extractors


import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.feature.Word2VecModel

import org.apache.spark.sql.SparkSession

import scalax.file.Path

/**
 * 1. IDE에서 local 실행....
 *   1.1 run : Run As > Scala Application
 *   
 */
object Word2VecExample1 {
  def main(args: Array[String]) {
    
    try(Path (this.getClass.getName).deleteRecursively(continueOnFailure = false)) catch {case e: Exception =>}
    
    lab.common.config.Config.setHadoopHOME
    
    val spark = SparkSession
      .builder
      .appName("Word2Vec example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    //--Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    //--Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)
    result.take(3).foreach(println)
    
    val synonyms = model.findSynonyms("I", 5)
    synonyms.collect().foreach(println)
    
    
    //--Save and load model
    model.save(this.getClass.getName)
    val sameModel = Word2VecModel.load(this.getClass.getName)
    
    sameModel.getVectors.collect().foreach{row => 
      println(row)
      sameModel.findSynonyms(row.getString(0), 5).collect().foreach(r => println(row.getString(0) + " => " + r))
    }
    
    spark.stop()
  }
}
