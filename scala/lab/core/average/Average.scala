package lab.core.average

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * 1. 이름(name) 별 나이(age)의 평균값을 구하시오.
 * 	 1.1 BaseRDD 생성....한 줄(line)에 한 사람의 정보(name탭age)	=> sc.textFile(path)
 *   1.2 이름(name)과 나이(age) 탭("\t")으로 구분.... Array[name, age]	=> map(x.split("\t"))
 *   1.3 (name, (age, 1)) 튜플 생성....	=> map((x(0), (x(1).toInt, 1)))
 *   1.4 (name, (age합, 1합:개수)) 튜플 생성....	=> reduceByKey((x._1+y._1, x._2+y._2))
 *   1.5 (name, age합/1합:개수) 튜플 생성....	=> map((x._1, x._2._1/x._2._2))
 *   1.6 결과 출력.... => Array[(String, Int)].foreach(println)
 * 
 */
object Average {
  def main(args: Array[String]) {
    
    lab.common.config.Config.setHadoopHOME
    
    val peopleFile = "src/main/resources/people.info"
    
    val sc = new SparkContext("local[2]", "Average")  //--local execution (run on eclipse)....
    val averageRDD = sc.textFile(peopleFile)  //--BaseRDD 생성....한 줄(line)에 한 사람의 정보(name탭age)
                             .map { x => x.split("\t") }  //--이름(name)과 나이(age) 탭("\t")으로 구분.... Array[name, age]
                             .map { x => (x(0), (x(1).toInt, 1)) }  //--(name, (age, 1)) 튜플 생성....
                             .reduceByKey {(x, y) => (x._1 + y._1, x._2 + y._2)}  //--(name, (age합, 1합:개수)) 튜플 생성....
                             .map(x => (x._1, x._2._1/x._2._2))  //--(name, age합/1합:개수) 튜플 생성....
    averageRDD.collect().foreach {println}  //--결과 출력....
    
    sc.stop()
  }
}
