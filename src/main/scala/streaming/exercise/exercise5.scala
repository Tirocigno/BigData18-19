package streaming.exercise


import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

class exercise5 {
  val sc = new SparkContext()
  val ssc5 = new StreamingContext(sc, Seconds(10))
  val lines = ssc5.socketTextStream("137.204.72.240",9852,
    StorageLevel.MEMORY_AND_DISK_SER).window( Seconds(60), Seconds(10) )
  val hashtags = lines.filter(_.nonEmpty).map( _.split("\\|") ).map(_(2)).filter(_.nonEmpty).flatMap(_.split(", "))
  val hastagPopularity = hashtags.map(x => (x, 1)).reduceByKey(_ + _).map({case(k,v)=>(v,k)}).transform({_.sortByKey(ascending = false)})
  hastagPopularity.print()

  ssc5.stop(false)
}
