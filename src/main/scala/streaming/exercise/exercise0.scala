package streaming.exercise

import org.apache.spark.SparkContext

class exercise0 {

  import org.apache.spark.streaming._
   import org.apache.spark.storage.StorageLevel
  val sc = new SparkContext();
   val ssc = new StreamingContext(sc, Seconds(3))
  val lines = ssc.socketTextStream("137.204.72.240",9852,
    StorageLevel.MEMORY_AND_DISK_SER)
   val words = lines.flatMap(_.split(" "))
   val count = words.count()
   count.print()
   ssc.start()
   ssc.stop(false)
}
