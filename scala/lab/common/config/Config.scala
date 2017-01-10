package lab.common.config

object Config {
  val PROJECT_HOME = "C:\\Scala_IDE_for_Eclipse\\eclipse\\workspace\\Spark2.0.2_Edu_Lab\\"
  val SPARK_HOME = PROJECT_HOME + "spark-2.0.2-bin-hadoop2.7"
  val APP_RESOURCE = PROJECT_HOME + "target\\Spark2.0.2_Edu_Lab-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
  
  def setHadoopHOME = {
    System.setProperty("hadoop.home.dir", PROJECT_HOME + "hadoop_home");
  }
}