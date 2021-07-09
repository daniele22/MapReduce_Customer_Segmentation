package ScalaSparkDBSCAN.util.collection

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable


private [ScalaSparkDBSCAN] class SynchronizedArrayBuffer [T]
  extends ArrayBuffer[T] with mutable.SynchronizedBuffer[T] with Serializable {

}
