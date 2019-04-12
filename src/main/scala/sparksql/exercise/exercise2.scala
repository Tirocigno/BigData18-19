package sparksql.exercise

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import sparksql.exercise.exercise1

class exercise2 {

  val sc = new SparkContext()
  val sqlContext = new SQLContext(sc)

  val populationDataFrame = exercise1.populationDataFrame

  populationDataFrame.write.json("/output/")


  //TODO SEE THE SOLUTION NON VA NEMMENO A PAGARE
  val  userDataParquetDF = exercise1.userDataParquetDF

  //import org.apache.spark.sql.hive.HiveContext;

 // HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc.sc());

  userDataParquetDF.write.saveAsTable("userData")

  val moviesJSON = exercise1.moviesJSON

  moviesJSON.write.parquet("./output/parquet")
  moviesJSON.write.saveAsTable("movies")


  moviesJSON.write.mode(SaveMode.Overwrite).saveAsTable("movies")

  //username & pw per database: amordenti



}
