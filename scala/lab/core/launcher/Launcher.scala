package lab.core.launcher

import org.apache.spark.launcher.SparkLauncher

/**
 * 1. IDE에서 local 실행....
 *   1.1 build : Spark2.0.0_Edu_Lab > Run As > Maven install
 *   1.2 Run As > Scala Application
 * 
 */
object Launcher {
  
  def main(args: Array[String]): Unit = {
    
    lab.common.config.Config.setHadoopHOME
    
    val spark = new SparkLauncher()
                        .setSparkHome(lab.common.config.Config.SPARK_HOME)
                        .setAppResource(lab.common.config.Config.APP_RESOURCE)
                        .setMainClass("lab.core.launcher.SparkApp")
                        .setMaster("local[2]")
                        .launch()
    /*
    val inputStreamReaderRunnable = new InputStreamReaderRunnable(spark.getInputStream(), "input");
    val inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
    inputThread.start();
    
    val errorStreamReaderRunnable = new InputStreamReaderRunnable(spark.getErrorStream(), "error");
    val errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
    errorThread.start();
    */
    
    println("Waiting for finish...");
    val exitCode = spark.waitFor();
    println("Finished! Exit code : " + exitCode)
  }
  
}