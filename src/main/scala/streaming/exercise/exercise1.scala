package streaming.exercise
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel


class exercise1 {
  val sc = new SparkContext();
  val ssc = new StreamingContext(sc, Seconds(3))
  val lines = ssc.socketTextStream("137.204.72.240",9852,
    StorageLevel.MEMORY_AND_DISK_SER)
  val words = lines.flatMap(_.split(" "))
   val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _).map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) })
   wordCounts.print()

}
