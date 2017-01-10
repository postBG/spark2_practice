package lab.core.customapi

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}


class DiscountRateRDD(prev:RDD[SalesRecord], discountRate:Double) extends RDD[SalesRecord](prev) {
  
  override def compute(split: Partition, context: TaskContext): Iterator[SalesRecord] =  {
    firstParent[SalesRecord].iterator(split, context).map(salesRecord => {
      val discount = salesRecord.itemValue * discountRate
      new SalesRecord(salesRecord.transactionId, salesRecord.customerId, salesRecord.itemId, discount)
    })
  }

  override protected def getPartitions: Array[Partition] = firstParent[SalesRecord].partitions
}

class DiscountPercentageRDD(prev:RDD[SalesRecord], discountPercentage:Double) extends RDD[SalesRecord](prev) {
  
  override def compute(split: Partition, context: TaskContext): Iterator[SalesRecord] =  {    
    firstParent[SalesRecord].iterator(split, context).map(salesRecord => {
      val discount = salesRecord.itemValue * (discountPercentage / 100)
      new SalesRecord(salesRecord.transactionId, salesRecord.customerId ,salesRecord.itemId, discount)
    })
  }

  override protected def getPartitions: Array[Partition] = firstParent[SalesRecord].partitions
}