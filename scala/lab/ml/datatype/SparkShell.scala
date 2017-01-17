package lab.ml.datatype

/**
 * 
 1. Spark Shell 연결....
  - # ./bin/spark-shell --master spark://CentOS7-14:7077
  
 2. Data Types - Vector....
  - scala> import org.apache.spark.mllib.linalg.{Vector, Vectors}
  - scala> val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
  - scala> val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
  - scala> val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
  
 3. Data Types - LabeledPoint....
  - scala> import org.apache.spark.mllib.linalg.Vectors
  - scala> import org.apache.spark.mllib.regression.LabeledPoint
  - scala> val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
  - scala> val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
  
 4. Data Types - LabeledPoint (LIBSVM format)....
  - scala> import org.apache.spark.mllib.regression.LabeledPoint
  - scala> import org.apache.spark.mllib.util.MLUtils
  - scala> import org.apache.spark.rdd.RDD
  - scala> val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
  - scala> examples.take(1).foreach(println)		//--파일(sample_libsvm_data.txt) 내용과 비교....
  
 5. Data Types - Rating....
  - scala> import org.apache.spark.mllib.recommendation.ALS
  - scala> import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
  - scala> import org.apache.spark.mllib.recommendation.Rating
  - scala> val data = sc.textFile("data/mllib/als/test.data")
  - scala> val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
  Rating(user.toInt, item.toInt, rate.toDouble)
  })
  - scala> ratings.take(1).foreach(println)		//--파일(test.data) 내용과 비교....
  
 6. Data Types - Local Matrix....
  - scala> import org.apache.spark.mllib.linalg.{Matrix, Matrices}
  - scala> val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))		//--column-major order
  - scala> val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))		//--3 x 2 CSCMatrix
  
 7. Data Types - RowMatrix....
  - scala> import org.apache.spark.mllib.linalg.Vector
  - scala> import org.apache.spark.mllib.linalg.distributed.RowMatrix
  - scala> val data = Array(
  Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
  Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
  Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
  )
  - scala> val rows: RDD[Vector] = sc.parallelize(data, 2)
  - scala> val mat: RowMatrix = new RowMatrix(rows)
  - scala> val m = mat.numRows()
  - scala> val n = mat.numCols()
  
 * 
 */
object SparkShell {
  
}