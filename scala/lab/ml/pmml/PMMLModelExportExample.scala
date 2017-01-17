package lab.ml.pmml

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

import scalax.file.Path

/**
 * 1. IDE에서 local 실행....
 *   1.1 run : Run As > Scala Application
 *   
 */
object PMMLModelExportExample {

  def main(args: Array[String]): Unit = {
    
    try(Path (this.getClass.getName).deleteRecursively(continueOnFailure = false)) catch {case e: Exception =>}

    lab.common.config.Config.setHadoopHOME
    
    val conf = new SparkConf().setAppName("PMMLModelExportExample").setMaster("local[*]")
    conf.set("spark.sql.warehouse.dir", "spark-warehouse")
    val sc = new SparkContext(conf)

    
    //--Load and parse the data
    val data = sc.textFile("data/mllib/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    //--Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    //--Export to PMML to a String in PMML format
    println(">>>> PMML Model:\n" + clusters.toPMML)
    
    //--Export the model to a directory on a distributed file system in PMML format
    clusters.toPMML(sc, this.getClass.getName + "/kmeans")

    //--Export the model to a local file in PMML format
    clusters.toPMML(this.getClass.getName + "/kmeans.xml")
    
    //--Export the model to the OutputStream in PMML format
    clusters.toPMML(System.out)
    

    sc.stop()
  }
}
