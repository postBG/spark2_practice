package lab.core.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 1. Word Count 구현....
 *   1.1 최종 전체 데이터 로그 출력 => RDD.foreach(f: (T) ⇒ Unit): Unit 
 *   1.2 최종 전체 데이터 파일 저장 => RDD.saveAsTextFile(path: String): Unit 
 *   1.3 최종 10개 데이터 로그 출력 => RDD.take(num: Int): Array[T]
 *   
 * 2. IDE에서 local 실행....
 *   2.1 run config : Run > Run Configurations > Arguments > Program arguments => C:\Scala_IDE_for_Eclipse\eclipse\workspace\Spark2.0.0_Edu_Lab\spark-2.0.0-bin-hadoop2.7\README.md local[2]
 *   2.2 run : Run As > Scala Application
 *   
 * 3. spark-submit으로 remote 실행....
 *   3.1 build : Spark2.0.0_Edu_Lab > Run As > Maven install
 *   3.2 remote upload : CentOS7-14 SSH 연결(MobaXterm) > # cd /kikang/spark-2.0.0-bin-hadoop2.7/dev/app > Spark2.0.0_Edu_Lab-0.0.1-SNAPSHOT-jar-with-dependencies.jar 파일 업로드
 *   3.3 run : CentOS7-14 SSH 연결(MobaXterm) > # cd /kikang/spark-2.0.0-bin-hadoop2.7 > # ./bin/spark-submit --master spark://CentOS7-14:7077 --class lab.core.wordcount.WordCount ./dev/app/Spark2.0.0_Edu_Lab-0.0.1-SNAPSHOT-jar-with-dependencies.jar README.md
 * 
 */
object WordCount {
  
  def main(args: Array[String]): Unit = {
    
    lab.common.config.Config.setHadoopHOME
    
    var file = "README.md"
    var master = "local[*]"
    if(args.size == 1) {
      file = args(0).toString()
    } else if(args.size == 2) {
      file = args(0).toString()
      master = args(1).toString()
    } 
    println("file : " + file)
    println("master : " + master)
    
    val conf=new SparkConf().setAppName("WordCount")
    
    try {      
      println("[Before] spark.master : " + conf.get("spark.master"))
    } catch {
      case e: java.util.NoSuchElementException => conf.setMaster(master)
    }
    println("[After] spark.master : " + conf.get("spark.master"))
    
    val sc=new SparkContext(conf)
    
    //--File로부터 BaseRDD 생성....(line 단위)
    val baseRDD=sc.textFile(file)
    
    //--Line을 단어로 구분하여 단어로 구성된 RDD 생성....
    val splitedRDD = baseRDD.flatMap { x => x.split(" ") }
    
    //--(단어, 1) 튜플 생성....
    val tupleRDD = splitedRDD.map { x => (x, 1) }
    
    //--단어별 개수 카운팅....
    val reducedRDD = tupleRDD.reduceByKey { (x, y) => x + y}
   
    //--로그 출력....   
    reducedRDD.foreach { x => println("word_count : " + x) }
    
    //--파일 저장....
    reducedRDD.saveAsTextFile(this.getClass.getName)
    
    //--10개만 로그 출력....
    reducedRDD.take(10).foreach { x => println("word_count(10 only) : " + x) }
    
    sc.stop()  
  }
  
}