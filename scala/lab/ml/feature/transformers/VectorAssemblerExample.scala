package lab.ml.feature.transformers

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.sql.SparkSession

import scalax.file.Path

/**
 * 1. IDE에서 local 실행....
 *   1.1 run : Run As > Scala Application
 *   
 */
object VectorAssemblerExample {
  def main(args: Array[String]): Unit = {
    
    try(Path (this.getClass.getName).deleteRecursively(continueOnFailure = false)) catch {case e: Exception =>}
    
    lab.common.config.Config.setHadoopHOME
    
    val spark = SparkSession
      .builder
      .appName("VectorAssemblerExample")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    val dataset = spark.createDataFrame(
      Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures"))
      .setOutputCol("features")

    val output = assembler.transform(dataset)
    println(output.select("features", "clicked").first())
    
    spark.stop()
  }
}
