package streaming.basic.exercise

import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

class exercise0 {

  /*Not necessary to copy inside the spark shell, already installed, need to import library though*/
  val sc = new SparkContext()
   val ssc = new StreamingContext(sc, Seconds(3))
  val lines = ssc.socketTextStream("137.204.72.240",9852,
    StorageLevel.MEMORY_AND_DISK_SER)
   val words = lines.flatMap(_.split(" "))
   val count = words.count()
   count.print()
   ssc.start()
   ssc.stop(false)
}
