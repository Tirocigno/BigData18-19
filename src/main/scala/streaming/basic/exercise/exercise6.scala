package streaming.basic.exercise

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

class exercise6 {
  val sc = new SparkContext()
  def functionToCreateContext(): StreamingContext = {
    val ssc6 = new StreamingContext(sc, Seconds(10))
    val lines = ssc6.socketTextStream("137.204.72.240",9852,
      StorageLevel.MEMORY_AND_DISK_SER)
    val cities = lines.filter(_.nonEmpty).map( _.split("\\|") ).map(_(4)).filter(_.nonEmpty).flatMap(_.split(", "))
    val cumulativeCityCounts = cities.map(x => (x, 1)).updateStateByKey(updateFunction)
      cumulativeCityCounts.map({case(k,v)=>(v,k)})
      .transform({ rdd => rdd.sortByKey(false) }).print()


    ssc6.checkpoint("hdfs:/user/fnaldini/streaming/checkpoint")
     ssc6
       }

   def updateFunction( newValues: Seq[Int], oldValue: Option[Int] ): Option[Int] = {
     Some( oldValue.getOrElse(0) + newValues.sum )
     }

  val ssc6 = StreamingContext.getOrCreate(
    "hdfs:/user/fnaldini/streaming/checkpoint",
    this.functionToCreateContext)


  ssc6.stop(false)
}
