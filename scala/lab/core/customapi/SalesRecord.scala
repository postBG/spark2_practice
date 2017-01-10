package lab.core.customapi

class SalesRecord(val transactionId: String,
                          val customerId: String,
                          val itemId: String,
                          val itemValue: Double) extends Comparable[SalesRecord] with Serializable {

  override def compareTo(o: SalesRecord): Int = {
    return this.transactionId.compareTo(o.transactionId)
  }

  override def toString: String = {
    s"$transactionId|$customerId|$itemId|$itemValue"
  }
}