package lab.workshop.recommendation

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.mllib.evaluation.RegressionMetrics

import org.jblas.DoubleMatrix

import scalax.file.Path


/**
 * 추천 알고리즘 인 ALS를 이용하여 모델 생성 및 추천 실행....
 * 학습 데이터 탐색 및 정련 > 모델 생성 > 모델 평가 > 추천 > 추천 (Item-based) 순으로 진행....
 * 
 * 학습 데이터 => Music Listening Dataset (Audioscrobbler.com => http://www.last.fm/api, 6 May 2005, for around 150,000 real people, Audioscrobbler is receiving around 2 million song submissions per day)
 * scrobble	미국·영국 [|skrɒbəl]  [VERB] (of an online music service) to record a listener's musical preferences and recommend similar music that he or she might enjoy
- user_artist_data.txt
    3 columns: userid artistid playcount

- artist_data.txt
    2 columns: artistid artist_name

- artist_alias.txt
    2 columns: badid, goodid
    known incorrectly spelt artists and the correct artist id. 
    you can correct errors in user_artist_data as you read it in using this file
 * 
 * 1. IDE에서 local 실행....
 *   1.1 run config : Run > Run Configurations > Arguments > VM arguments => -Xms4096m -Xmx4096m (실습환경에서 허용하는 범위 내에서 가능한 많은 메모리 할당....)
 *   1.2 run : Run As > Scala Application
 */
object A1Recommender {

  def main(args: Array[String]): Unit = {    
    
    lab.common.config.Config.setHadoopHOME
    
    //--SparkContext 생성....
    val conf = new SparkConf().setAppName("A1Recommender").setMaster("local[*]")
    conf.set("spark.sql.warehouse.dir", "spark-warehouse")
    val sc = new SparkContext(conf)
       
    
    //--학습 데이터 경로....(on HDFS)
    //val base = "hdfs:///user/ds/"
    val base = "src/main/resources/profiledata_06-May-2005/"
    
    val rawUserArtistData = sc.textFile(base + "user_artist_data.txt")  //--user_artist_data.txt (3 columns: userid artistid playcount)
    val rawArtistData = sc.textFile(base + "artist_data.txt")  //--artist_data.txt (2 columns: artistid artist_name)
    val rawArtistAlias = sc.textFile(base + "artist_alias.txt")  //--artist_alias.txt (2 columns: badid, goodid)
    
    
    //--LAB 1. (일부) 데이터 탐색, 데이터 정련....
    //preparation(rawUserArtistData, rawArtistData, rawArtistAlias)    
    
    //--LAB 2. 모델 생성....
    //model(sc, rawUserArtistData, rawArtistData, rawArtistAlias)
    
    //--LAB 3. 모델 평가....
    //evaluate(sc, rawUserArtistData, rawArtistAlias)    
    
    //--LAB 4. 추천....
    //recommend(sc, rawUserArtistData, rawArtistData, rawArtistAlias)
    
    //--LAB 5. 추천....(Item-based)....
    recommendByItembased(sc, rawUserArtistData, rawArtistData, rawArtistAlias)
  }
    
    
  
  
  /*******************************************************************************************************************************************************
   * LAB 1. (일부) 데이터 탐색, 데이터 정련....
   *******************************************************************************************************************************************************/
  def preparation(
      rawUserArtistData: RDD[String],
      rawArtistData: RDD[String],
      rawArtistAlias: RDD[String]) = {
    
    //--데이터 탐색....
    //--ALS가 입력받는 user, product의 id 값은 Integer 만 가능함.... Intger 최소 최대 크기 만족 여부 확인....
    //--스파크 MLlib의 ALS 구현은 제약이 있어서 사용자와 아이템에 부여될 ID는 음(-)이 아닌 32비트 정수여야 한다.  => 즉, Integer.MAX_VALUE 보다 큰 값은 ID로 사용할 수 없다.      
    val userIDStats = rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
    val itemIDStats = rawUserArtistData.map(_.split(' ')(1).toDouble).stats()
    val playCountStats = rawUserArtistData.map(_.split(' ')(2).toDouble).stats()
    println(">>>> userIDStats : " + userIDStats)
    println(">>>> itemIDStats : " + itemIDStats)
    println(">>>> playCountStats : " + playCountStats)
    println(">>>> Integer.MAX_VALUE : " + Integer.MAX_VALUE)
    println(">>>> Integer.MIN_VALUE : " + Integer.MIN_VALUE)
    /*
    >>>> userIDStats : (count: 24296858, mean: 1947573.265353, stdev: 496000.544975, max: 2443548.000000, min: 90.000000)
    >>>> itemIDStats : (count: 24296858, mean: 1718704.093757, stdev: 2539389.040171, max: 10794401.000000, min: 1.000000)
    >>>> playCountStats : (count: 24296858, mean: 15.295762, stdev: 153.915321, max: 439771.000000, min: 1.000000)
    >>>> Integer.MAX_VALUE : 2147483647
    >>>> Integer.MIN_VALUE : -2147483648
    */
    
    //--한번이라도 들은 product 개수 구하기....
    val allItemIDs = rawUserArtistData.map(_.split(' ')(1).toDouble).distinct().collect()
    println(">>>> allItemIDs.length : " + allItemIDs.length)
    
    //--artistbyID는 RDD....
    val artistByID = buildArtistByID(rawArtistData)
    //--artistAlias는 Map....
    val artistAlias = buildArtistAlias(rawArtistAlias)
    
    //--아티스트 아이디(BadID, GoodID)로 아티스트 이름을 출력해 본다....
    val (badID, goodID) = artistAlias.head
    println(">>>> artistAlias.head : " + badID, goodID)
    println(">>>> artistAlias.head's Name(bad -> good) : " + artistByID.lookup(badID) + " -> " + artistByID.lookup(goodID))
  }
  
  
  //--user_artist_data.txt (3 columns: userid artistid playcount)
  //--Rating 데이터 구성....
  private def buildRatings(
      rawUserArtistData: RDD[String],
      bArtistAlias: Broadcast[Map[Int,Int]]) = {
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      //--BadID를 GoodID로 변경....
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }
  }
  
  
  //--artist_data.txt (2 columns: artistid artist_name)
  //--아티스트 정보는 탭으로 구분됨
  //--이름이 없는 경우도 있음, 이름중에 탭이 있는 경우도? => map 대신 flatMap 사용....Collection이나 Option 리턴 가능....
  private def buildArtistByID(rawArtistData: RDD[String]) =
    rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')  //--첫번째 탭을 기준으로 데이터를 양분함.... => 이름중에 탭이 있는 경우도?
      if (name.isEmpty) {  //--이름이 없는 경우....
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }
  
  //--artist_alias.txt (2 columns: badid, goodid)
  //--탭으로 구분....
  //--Alias가 없는 경우도 있음 => => map 대신 flatMap 사용....Collection이나 Option 리턴 가능....
  private def buildArtistAlias(rawArtistAlias: RDD[String]): Map[Int,Int] =
    rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()
    
  
    
    
  /*******************************************************************************************************************************************************
   * LAB 2. 모델 생성....
   *******************************************************************************************************************************************************/
  def model(
      sc: SparkContext,
      rawUserArtistData: RDD[String],
      rawArtistData: RDD[String],
      rawArtistAlias: RDD[String]): Unit = {
    //--artistAlias는 Map.... => Task별 데이터 전송 및 복제 비용 줄이기 위해 브로드캐스트 변수로 생성.... 
    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))
    
    //--ALS 알고리즘을 위해 학습데이터를 Rating 형태로 변형.... + 학습데이터는 캐시하시오....알고리즘에 의해 반복적으로 사용됨....
    val trainData = buildRatings(rawUserArtistData, bArtistAlias).cache()
    
    //--하이퍼파라미터 세팅하여 Implicit 데이터를 이용한 모델 트레이닝....
    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
    
    //--학습데이터 캐시 해제....
    trainData.unpersist()
    
    //--학습된 모델 출력해봄.... 잠재 특징(Feature) 출력해봄....
    //println(model.userFeatures.mapValues(_.mkString(", ")).first())
    //--added by kikang....
    model.userFeatures.mapValues(_.mkString(", ")).take(5).foreach(x => println(">>>> " + x))
    println("-------------------------------------------------------------------")
    model.productFeatures.mapValues(_.mkString(", ")).take(5).foreach(x => println(">>>> " + x))
    
    
    val userID = 2093760
    //val userID = 128
    
    //--특정 사용자의 잠재 특징 출력....
    model.userFeatures.lookup(userID)foreach{ row => 
      println(">>>> " + row.mkString(", "))      
    }
    
    //--특정 사용자를 위한 아티스트 5명 추천 목록 가져오기....
    val recommendations = model.recommendProducts(userID, 5)
    recommendations.foreach(x => println(">>>> " + x))
    //--모델에 의해 추천된 특정 사용자의 아티스트 아이디 목록....
    val recommendedProductIDs = recommendations.map(_.product).toSet

    val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).
      filter { case Array(user,_,_) => user.toInt == userID }
    //--기존 학습데이터에 있는 특정 사용자가 재생한 적이 있는 아티스트 아이디 목록....
    val existingProducts = rawArtistsForUser.map { case Array(_,artist,_) => artist.toInt }.
      collect().toSet
    
    //--아티스트 (아이디, 이름) RDD.... 
    val artistByID = buildArtistByID(rawArtistData)
    
    //--기존 학습데이터에 있는 특정 사용자가 재생한 적이 있는 아티스트 아이디 목록의 이름을 출력....PairRDD.values 사용....
    artistByID.filter { case (id, name) => existingProducts.contains(id) }.
      collect().foreach(x => println(">>>> " + x))
    println("-------------------------------------------------------------------")
    //--모델에 의해 추천된 특정 사용자의 아티스트 아이디 목록의 이름을 출력....PairRDD.values 사용....
    artistByID.filter { case (id, name) => recommendedProductIDs.contains(id) }.
      collect().foreach(x => println(">>>> " + x))
    
    //--모델(userFeatures, productFeatures 각 RDD) 캐시 해제....
    unpersist(model)
  }
  
  

  
  
  /*******************************************************************************************************************************************************
   * LAB 3. 모델 평가....
   *******************************************************************************************************************************************************/
  def evaluate(
      sc: SparkContext,
      rawUserArtistData: RDD[String],
      rawArtistAlias: RDD[String]): Unit = {
    
    val start = System.currentTimeMillis()
    
    //--데이터가 큰 변수를 각 task에서 참조하니까 브로드캐스트로 변수값을 한번 넘겨 재사용하도록 구현....
    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))
    
    //--재생 정보에 포함된 아티스트 알리아스의 아이디를 원래 아티스트 아이디로 변경하여 Rating 생성.... + 캐시....
    val allData = buildRatings(rawUserArtistData, bArtistAlias).cache()
    //val allData = buildRatings(rawUserArtistData.randomSplit(Array(0.0001, 0.9999), 11L)(0), bArtistAlias).cache()  //--원본의 0.01%에 해당하는 데이터를 전체 데이터로 간주하고 연산.... => 컴퓨터 리소스 부족 및 실습 소요시간 단축 위해....
    
    //--훈련데이터, 테스트 데이터 분리.... + 캐시....
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    cvData.cache()

    //--모든 product 목록 구해서 브로드캐스트 생성....
    val allItemIDs = allData.map(_.product).distinct().collect()
    val bAllItemIDs = sc.broadcast(allItemIDs)
    println(">>>> all product num : " + allItemIDs.length)
    
    
    //--아티스트별로 모든 사람들이 재생한 횟수를 추천 Rating으로 할 경우에도 나쁘지 않다... => 개인화하지 않은 추천 방식도 제법 효과적임....
    val mostListenedAUC = areaUnderCurve(cvData, bAllItemIDs, predictMostListened(sc, trainData))
    println(">>>> areaUnderCurve for predictMostListened : " + mostListenedAUC)
    
    
    //--하이퍼파라미터 조합별 평가...
    //--평가는 Area Under Curve (AUC), Mean Squared Error (MSE), Root Mean Squared Error (RMSE) 등이 사용 가능....
    val evaluations =
      /*
      //--조합이 8개(2 * 2 * 2)일 경우....
      for (rank   <- Array(10,  50);
           lambda <- Array(1.0, 0.0001);
           alpha  <- Array(1.0, 40.0))
      */
      //--조합 1개일 경우.... => 빠른 테스트를 위해 조합 1개로 테스트....
      for (rank   <- Array(2);
           lambda <- Array(1.0);
           alpha  <- Array(40.0))
      yield {
        //val model = ALS.trainImplicit(trainData, rank, 10, lambda, alpha)
        val model = ALS.trainImplicit(trainData, rank, 2, lambda, alpha)
        
        //--#01. Area Under Curve (AUC)
        val auc = areaUnderCurve(cvData, bAllItemIDs, model.predict)        
        unpersist(model)
        ((rank, lambda, alpha), auc)
        
        /*
        //--#02. Mean Squared Error (MSE)
        val mse = meanSquaredError(cvData, model.predict)        
        unpersist(model)
        ((rank, lambda, alpha), mse)
        */
        /*
        //--#03. Root Mean Squared Error (RMSE)
        val rmse = rootMeanSquaredError(cvData, model.predict)        
        unpersist(model)
        ((rank, lambda, alpha), rmse)
        */
        //((10,1.0,40.0),159.32690289069578)
      }
    
    //--#01. AUC 값으로 '내림차순' 정렬....
    val evaluationsResult = evaluations.sortBy(_._2).reverse    
    //--#02. MSE/RMSE 값으로 '오름차순' 정렬....
    //val evaluationsResult = evaluations.sortBy(_._2)
    evaluationsResult.foreach(x => println(">>>> " + x))

    trainData.unpersist()
    cvData.unpersist()
    
    
    //--Best 하이퍼파라미터 추출....
    val (bestRank, bestLambda, bestAlpha) = evaluationsResult.head._1
    //--Best 하이퍼파라미터 출력....
    println(s">>>> Best hyperparameter => Rank: ${bestRank}, Lambda: ${bestLambda}, Alpha: ${bestAlpha}")
    
    //--Best 하이퍼파라미터를 이용하여 전체 데이터로 모델을 학습....    
    val (blocks, seed) = (12, 11L)
    val bestModel = ALS.trainImplicit(allData, bestRank, 2, bestLambda, blocks, bestAlpha, seed)
    allData.unpersist()
    
    //--Best 모델 저장....
    try(Path (this.getClass.getName).deleteRecursively(continueOnFailure = false)) catch {case e: Exception =>}
    bestModel.save(sc, this.getClass.getName)
    unpersist(bestModel)
    
    println(">>>> Lap time : " + (System.currentTimeMillis() - start)) 
  }
  
  
  private def predictMostListened(sc: SparkContext, train: RDD[Rating])(allData: RDD[(Int,Int)]) = {
    val bListenCount =
      sc.broadcast(train.map(r => (r.product, r.rating)).reduceByKey(_ + _).collectAsMap())
    allData.map { case (user, product) =>
      Rating(user, product, bListenCount.value.getOrElse(product, 0.0))
    }
  }
    
  //--Area Under Curve (AUC)....
  private def areaUnderCurve(
      positiveData: RDD[Rating],
      bAllItemIDs: Broadcast[Array[Int]],
      predictFunction: (RDD[(Int,Int)] => RDD[Rating])) = {
    //--What this actually computes is AUC, per user. The result is actually something
    //--that might be called "mean AUC".

    //--Take held-out data as the "positive", and map to tuples
    val positiveUserProducts = positiveData.map(r => (r.user, r.product))
    //--Make predictions for each of them, including a numeric score, and gather by user
    val positivePredictions = predictFunction(positiveUserProducts).groupBy(_.user)

    //--BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
    //--small AUC problems, and it would be inefficient, when a direct computation is available.

    //--Create a set of "negative" products for each user. These are randomly chosen
    //--from among all of the other items, excluding those that are "positive" for the user.
    val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {
      //--mapPartitions operates on many (user,positive-items) pairs at once
      userIDAndPosItemIDs => {
        //--Init an RNG and the item IDs set once for partition
        val random = new Random()
        val allItemIDs = bAllItemIDs.value
        userIDAndPosItemIDs.map { case (userID, posItemIDs) =>
          val posItemIDSet = posItemIDs.toSet
          val negative = new ArrayBuffer[Int]()
          var i = 0
          //--Keep about as many negative examples per user as positive.
          //--Duplicates are OK
          while (i < allItemIDs.size && negative.size < posItemIDSet.size) {
            val itemID = allItemIDs(random.nextInt(allItemIDs.size))
            if (!posItemIDSet.contains(itemID)) {
              negative += itemID
            }
            i += 1
          }
          //--Result is a collection of (user,negative-item) tuples
          negative.map(itemID => (userID, itemID))
        }
      }
    }.flatMap(t => t)
    //--flatMap breaks the collections above down into one big set of tuples

    //--Make predictions on the rest:
    val negativePredictions = predictFunction(negativeUserProducts).groupBy(_.user)

    //--Join positive and negative by user
    val auc = positivePredictions.join(negativePredictions).values.map {
      case (positiveRatings, negativeRatings) =>
        //--AUC may be viewed as the probability that a random positive item scores
        //--higher than a random negative one. Here the proportion of all positive-negative
        //--pairs that are correctly ranked is computed. The result is equal to the AUC metric.
        var correct = 0L
        var total = 0L
        //--For each pairing,
        for (positive <- positiveRatings;
             negative <- negativeRatings) {
          //--Count the correctly-ranked pairs
          if (positive.rating > negative.rating) {
            correct += 1
          }
          total += 1
        }
        //--Return AUC: fraction of pairs ranked correctly
        correct.toDouble / total
    }.mean() //--Return mean AUC over users
    println(">>>> Area Under Curve = " + auc)
    auc
  }
  
  //--Mean Squared Error (MSE)....
  private def meanSquaredError( 
      cvData: RDD[Rating],
      predictFunction: (RDD[(Int,Int)] => RDD[Rating])) = {
    //--Evaluate the model on rating data
    val usersProducts = cvData.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions = predictFunction(usersProducts).map { case Rating(user, product, prediction) =>
        ((user, product), prediction)
      }
    val predsAndRates = predictions.join(cvData.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    })
    val MSE = predsAndRates.map { case ((user, product), (prediction, rate)) =>
      val err = (prediction - rate)
      err * err
      //math.pow(prediction - rate, 2.0)
    }.mean()
    
    //--MSE
    println(">>>> Mean Squared Error = " + MSE)
    MSE
  }
  
  //--Root Mean Squared Error (RMSE)....
  private def rootMeanSquaredError(
      cvData: RDD[Rating],
      predictFunction: (RDD[(Int,Int)] => RDD[Rating])) = {
    //--Evaluate the model on rating data
    val usersProducts = cvData.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions = predictFunction(usersProducts).map { case Rating(user, product, prediction) =>
        ((user, product), prediction)
      }
    val predsAndRates = predictions.join(cvData.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    })
    val predictionAndLabels = predsAndRates.map { case ((user, product), (prediction, rate)) =>
      (prediction, rate)
    }
    //--Get the RMSE using regression metrics
    val regressionMetrics = new RegressionMetrics(predictionAndLabels)
    println(s">>>> RMSE = ${regressionMetrics.rootMeanSquaredError}")
    
    //--MSE
    println(s">>>> MSE = ${regressionMetrics.meanSquaredError}")
    
    //--R-squared
    println(s">>>> R-squared = ${regressionMetrics.r2}")
    
    regressionMetrics.rootMeanSquaredError
  }
  
  
  
  /*******************************************************************************************************************************************************
   * LAB 4. 추천....
   *******************************************************************************************************************************************************/
  def recommend(
      sc: SparkContext,
      rawUserArtistData: RDD[String],
      rawArtistData: RDD[String],
      rawArtistAlias: RDD[String]): Unit = {
    
    val start = System.currentTimeMillis()
    
    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))
    val allData = buildRatings(rawUserArtistData, bArtistAlias).cache()
    //val model = ALS.trainImplicit(allData, 50, 10, 1.0, 40.0)
    //val model = ALS.trainImplicit(allData, 2, 2, 1.0, 40.0)
    val (blocks, seed) = (12, 11L)
    val model = ALS.trainImplicit(allData, 2, 2, 1.0, blocks, 40.0, seed)
    allData.unpersist()

    val userID = 2093760
    val recommendations = model.recommendProducts(userID, 5)
    val recommendedProductIDs = recommendations.map(_.product).toSet

    val artistByID = buildArtistByID(rawArtistData)

    artistByID.filter { case (id, name) => recommendedProductIDs.contains(id) }.
       collect().foreach(x => println(">>>> " + x))
       
    val someUsers = allData.map(_.user).distinct().take(100)
    val someRecommendations = someUsers.map(userID => model.recommendProducts(userID, 5))
    someRecommendations.map(
      recs => recs.head.user + " -> " + recs.map(_.product).mkString(", ")
    ).foreach(x => println(">>>> " + x))
    
    unpersist(model)
    
    //--저장된 Best 모델을 로드하여 추천 서비스....
    val bestModel = MatrixFactorizationModel.load(sc, this.getClass.getName)
    val recommendations2 = bestModel.recommendProducts(userID, 5)
    val recommendedProductIDs2 = recommendations2.map(_.product).toSet

    val artistByID2 = buildArtistByID(rawArtistData)

    artistByID2.filter { case (id, name) => recommendedProductIDs2.contains(id) }.
       collect().foreach(x => println(">>>> " + x))
    
    unpersist(bestModel)
    
    println(">>>> Lap time : " + (System.currentTimeMillis() - start)) 
  }

  
  private def unpersist(model: MatrixFactorizationModel): Unit = {
    //--At the moment, it's necessary to manually unpersist the RDDs inside the model
    //--when done with it in order to make sure they are promptly uncached
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }
  
  
  
  
  
  
  /*******************************************************************************************************************************************************
   * LAB 5. 추천....(Item-based)....
   * 
   * 현재 MatrixFactorizationModel API는 제품과 제품에 대한 유사성 연산 기능을 직접적으로 지원하지 않음....
   * 따라서, 별도의 유사성 연산 기능을 구현해야 함....
   * 대부분의 경우 유사성은 두 제품의 벡터 표현을 비교해서 계산됨....
   * 일반적인 유사성은 실제 값 벡터를 활용하는 피어슨 상관관계(Pearson Correlation), 코사인 유사성(Cosine Similarity)과 
   * 바이너리 벡터를 활용하는 자카드 유사성(Jaccard Similarity) 기법으로 측정함....
   * 여기서는 코사인 유사성(Cosine Similarity) 기법을 사용할 것임. 이때 jblas 선형 대수 라이브러리 활용함....
   *******************************************************************************************************************************************************/
  def recommendByItembased(
      sc: SparkContext,
      rawUserArtistData: RDD[String],
      rawArtistData: RDD[String],
      rawArtistAlias: RDD[String]): Unit = {
    
    val start = System.currentTimeMillis()
    
    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))
    val allData = buildRatings(rawUserArtistData, bArtistAlias).cache()
    //val model = ALS.trainImplicit(allData, 50, 10, 1.0, 40.0)
    //val model = ALS.trainImplicit(allData, 2, 2, 1.0, 40.0)
    val (blocks, seed) = (12, 11L)
    val model = ALS.trainImplicit(allData, 2, 2, 1.0, blocks, 40.0, seed)
    allData.unpersist()
    
    val itemId = 1244723
    val itemFactor = model.productFeatures.lookup(itemId).head 
    val itemVector = new DoubleMatrix(itemFactor)
    println(">>>> Same product cosineSimilarity : " + cosineSimilarity(itemVector, itemVector))
    
    val sims = model.productFeatures.map{ case (id, factor) => 
    	val factorVector = new DoubleMatrix(factor)
    	val sim = cosineSimilarity(factorVector, itemVector)
    	(id, sim)
    }
    val K = 10
    val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    println(">>>> " + sortedSims.mkString("\n>>>> "))
    
    //--artistbyID는 RDD....
    val artistByID = buildArtistByID(rawArtistData)
    println(s">>>> itemId : ${itemId}, artist name : " + artistByID.lookup(itemId).mkString(", "))
        
    val sortedSimsPlusOne = sims.top(K + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    val sortedSimsExceptSelf = sortedSimsPlusOne.slice(1, sortedSimsPlusOne.length).map{ case (id, sim) => (id, artistByID.lookup(id).mkString(", "), sim) }
    println(">>>> " + sortedSimsExceptSelf.mkString("\n>>>> "))
    
    unpersist(model)
    
    
    
    
    //--저장된 Best 모델을 로드하여 추천 서비스....
    val bestModel = MatrixFactorizationModel.load(sc, this.getClass.getName)
    val itemFactor2 = bestModel.productFeatures.lookup(itemId).head 
    val itemVector2 = new DoubleMatrix(itemFactor2)
    println(">>>> Same product cosineSimilarity : " + cosineSimilarity(itemVector2, itemVector2))
    
    val sims2 = bestModel.productFeatures.map{ case (id, factor) => 
    	val factorVector = new DoubleMatrix(factor)
    	val sim = cosineSimilarity(factorVector, itemVector)
    	(id, sim)
    }
    
    val sortedSims2 = sims2.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    println(">>>> " + sortedSims2.mkString("\n>>>> "))
    
    //--artistbyID는 RDD....
    val artistByID2 = buildArtistByID(rawArtistData)
    println(s">>>> itemId : ${itemId}, artist name : " + artistByID2.lookup(itemId).mkString(", "))
        
    val sortedSimsPlusOne2 = sims2.top(K + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    val sortedSimsExceptSelf2 = sortedSimsPlusOne2.slice(1, sortedSimsPlusOne2.length).map{ case (id, sim) => (id, artistByID2.lookup(id).mkString(", "), sim) }
    println(">>>> " + sortedSimsExceptSelf2.mkString("\n>>>> "))

    unpersist(bestModel)
    
    println(">>>> Lap time : " + (System.currentTimeMillis() - start)) 
  }
  
  //--코사인 유사성(Cosine Similarity) 메소드 구현....
  //--두 벡터 간 내적을 계산하고 각 벡터의 기준(혹은 길이)을 곱한 분모로 결과를 나눔....
  //--코사인 유사성은 -1과 1 사이의 값으로 측정.....
  //--1은 완전히 유사한 함. 0은 독립적(즉, 유사성 없음). -1은 유사하지 않을 뿐 아니라 완전히 다름.
  private def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
  	vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }

}