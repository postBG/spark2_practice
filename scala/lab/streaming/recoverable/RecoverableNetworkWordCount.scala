package lab.streaming.recoverable

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.util.{IntParam, LongAccumulator}
import org.apache.spark.streaming._


//--Use this singleton to get or register a Broadcast variable....
object WordBlacklist {
  
  @volatile private var instance: Broadcast[Seq[String]] = null
  
  def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val wordBlacklist = Seq("a", "b", "c")
          instance = sc.broadcast(wordBlacklist)
        }
      }
    }
    instance
  }
}

//--Use this singleton to get or register an Accumulator....
object DroppedWordsCounter {
  
  @volatile private var instance: LongAccumulator = null

  def getInstance(sc: SparkContext): LongAccumulator = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.longAccumulator("WordsInBlacklistCounter")
        }
      }
    }
    instance
  }
}


/**
 * Counts words in text encoded with UTF8 received from the network every 2 seconds. This example also
 * shows how to use lazily instantiated singleton instances for Accumulator and Broadcast so that
 * they can be registered on driver failures.
 * 
 * 1. Socket Server(Netcat) 실행....
 *   1.1 CentOS7-14 SSH 연결(MobaXterm)
 *   1.2 # cd /kikang/spark-2.0.2-bin-hadoop2.7
 *   1.3 # yum install nc		//--Netcat(nc) 설치....(필요시....)
 *   1.4 # nc -lk 9999				//--Netcat 실행(port 9999)....   
 *   1.5 # Netcat 콘솔에 데이터 입력....	//--Netcat 데이터 송신....
 *   
 * 2. IDE에서 local 실행.... + Netcat 데이터 송신....
 *   2.1 run : Run As > Scala Application
 *   2.2 # Netcat 콘솔에 데이터 입력....	//--Netcat 데이터 송신....=> "a", "b", "c" 위주로 입력....
 *   
 */
object RecoverableNetworkWordCount {

  def createContext(ip: String, port: Int, outputPath: String, checkpointDirectory: String): StreamingContext = {

    //--If you do not see this printed, that means the StreamingContext has been loaded
    //--from the new checkpoint
    println(">>>> Creating New Context........")
    
    val outputFile = new File(outputPath)
    if (outputFile.exists()) outputFile.delete()
    
    val sparkConf = new SparkConf().setAppName("RecoverableNetworkWordCount").setMaster("local[*]")
    //--Create the context with a 2 seconds batch size
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint(checkpointDirectory)
    
    //--Create a socket stream on target ip:port and count the
    //--words in input stream of \n delimited text (eg. generated by 'nc')
    val lines = ssc.socketTextStream(ip, port)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
    
    
    
    
    //--0. Accumulator....=> 복구(X)....
    wordCounts.foreachRDD { (rdd: RDD[(String, Int)], time: Time) =>
      //--Get or register the blacklist Broadcast
      val blacklist = WordBlacklist.getInstance(rdd.sparkContext)
      //--Get or register the droppedWordsCounter Accumulator
      val droppedWordsCounter = DroppedWordsCounter.getInstance(rdd.sparkContext)
      //--Use blacklist to drop words and use droppedWordsCounter to count them
      val counts = rdd.filter { case (word, count) =>
        if (blacklist.value.contains(word)) {
          droppedWordsCounter.add(count)
          false
        } else {
          true
        }
      }.collect().mkString("[", ", ", "]")
      val output = ">>>> Counts at time [" + time + "] " + counts
      println(output)
      
      //--0. Accumulator....=> 복구(X)....
      println(">>>> [0] [Accumulator] Dropped (" + droppedWordsCounter.value + ") word(s) totally")
      
      //println(">>>> Appending to " + outputFile.getAbsolutePath)
      Files.append(output + "\n", outputFile, Charset.defaultCharset())      
    }
    
    
    
    /*
    //--1. Function for updateStateByKey....=> 복구(O)....
    val computeRunningSum = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }      
    //--updateStateByKey....
    val updateStateByKeyDstream = wordCounts.updateStateByKey(computeRunningSum)
    //updateStateByKeyDstream.print()
    updateStateByKeyDstream.foreachRDD{ (rdd, time) => rdd.foreach{ row => println(">>>> [1] [updateStateByKey] [" + time + "] " + row) } }
    */
       
    
    /*
    //--2. Function for mapWithState....=> 복구(O)....
    //--A mapping function that maintains an integer state and return a (String, Int) tuple....
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word + "_by_mapWithState", sum)
      //--Use state.exists(), state.get(), state.update() and state.remove()
      //--to manage state, and return the necessary string
      state.update(sum)
      output
    }
    //--mapWithState.... => PairDStreamFunctions...
    val mapWithStateDstream = wordCounts.mapWithState(StateSpec.function(mappingFunc).numPartitions(5))
    //mapWithStateDstream.print()
    mapWithStateDstream.foreachRDD{ (rdd, time) => rdd.foreach{ row => println(">>>> [2] [mapWithState] [" + time + "] " + row) } }
    */
    
    
    /*
    //--3. window....=> 복구(X)....
    val windowDStream = wordCounts.window(Seconds(600), Seconds(2))
    val reducedWindowDStream = windowDStream.reduceByKey(_+_)
    //reducedWindowDStream.print()
    reducedWindowDStream.foreachRDD{ (rdd, time) => rdd.foreach{ row => println(">>>> [3] [window] [" + time + "] " + row) } }
    */
    
    
    /*
    //--4. reduceByKeyAndWindow....=> 복구(X)....
    val reduceByKeyAndWindowDStream = wordCounts.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(600), Seconds(2)) 
    //reduceByKeyAndWindowDStream.print()
    reduceByKeyAndWindowDStream.foreachRDD{ (rdd, time) => rdd.foreach{ row => println(">>>> [4] [reduceByKeyAndWindow] [" + time + "] " + row) } }
    */
    
    
    /*
    //--5. reduceByKeyAndWindow (with inverse function)....=> 복구(O)....
    val reduceByKeyAndWindowWithInverseDStream = wordCounts.reduceByKeyAndWindow(_ + _, _ - _, Seconds(600), Seconds(2), 4)
    //reduceByKeyAndWindowWithInverseDStream.print()
    reduceByKeyAndWindowWithInverseDStream.foreachRDD{ (rdd, time) => rdd.foreach{ row => println(">>>> [5] [reduceByKeyAndWindow (with inverse function)] [" + time + "] " + row) } }
    */
    
    ssc
  }

  def main(args: Array[String]) {
    
    lab.common.config.Config.setHadoopHOME
   
    val Array(ip, port, checkpointDirectory, outputPath) = Array("CentOS7-14", "9999", "checkpoint", this.getClass.getName)
       
   /* If the directory <checkpoint> does not exist (e.g. running for the first time), it will create
    * a new StreamingContext (will print "Creating new context" to the console). Otherwise, if
    * checkpoint data exists in <checkpoint>, then it will create StreamingContext from
    * the checkpoint data.*/
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, () => createContext(ip, port.toInt, outputPath, checkpointDirectory))
      
    ssc.start()
    ssc.awaitTermination()
  }
}