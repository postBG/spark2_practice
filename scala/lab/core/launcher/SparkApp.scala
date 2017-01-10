package lab.core.launcher

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkApp {
  
  def main(args: Array[String]): Unit = {
    
    lab.common.config.Config.setHadoopHOME
    
    val conf=new SparkConf().setAppName("SparkApp")
    val sc=new SparkContext(conf)
    val rdd=sc.parallelize(Array(2,3,2,1))
    rdd.foreach { x => println("rdd.foreach.x : " + x) }
    rdd.saveAsTextFile(this.getClass.getName) 
    sc.stop()
  }
  
}