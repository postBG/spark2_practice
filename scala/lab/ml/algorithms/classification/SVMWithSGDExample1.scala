package lab.ml.algorithms.classification

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.mllib.optimization.L1Updater 

import scalax.file.Path

/**
 * 1. IDE에서 local 실행....
 *   1.1 run : Run As > Scala Application
 *   
 */
object SVMWithSGDExample1 {

  def main(args: Array[String]): Unit = {
    
    try(Path (this.getClass.getName).deleteRecursively(continueOnFailure = false)) catch {case e: Exception =>}

    lab.common.config.Config.setHadoopHOME
    
    val conf = new SparkConf().setAppName("SVMWithSGDExample").setMaster("local[*]")
    conf.set("spark.sql.warehouse.dir", "spark-warehouse")
    val sc = new SparkContext(conf)

    //--Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

    //--Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    
    /*
    //--Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)
    */
    
    //--Run training algorithm to build the model
    val svmAlg = new SVMWithSGD() 

    svmAlg.optimizer 
        .setNumIterations(200) 
        .setRegParam(0.1) 
        .setUpdater(new L1Updater) 
    
    val model = svmAlg.run(training) 


    //--Clear the default threshold.
    //model.clearThreshold()
    println("Threshold : " + model.getThreshold)
    model.setThreshold(0.5)
    println("Threshold : " + model.getThreshold)
    
    //--Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    
    scoreAndLabels.collect().foreach(println)
    
    //--Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    //--Save and load model
    model.save(sc, this.getClass.getName)
    val sameModel = SVMModel.load(sc, this.getClass.getName)
    
    sc.stop()
  }
}
