package sparksql.exercise

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._


object exercise1 {
  /*JSON LOADING*/
  val sc = new SparkContext()
  val sqlContext = new SQLContext(sc)
  /*JSON LOADING*/
  val moviesJSON = sqlContext.jsonFile("/bigdata/dataset/movies")

  /*CSV FILE*/
  val populationCSV = sc.textFile("/bigdata/dataset/population/zipcode_population_no_header.csv")
  val populationSchema = "zipcode avgAge totPopulation males females"

  val populationSchemaType = StructType(populationSchema.split(" ").map(fieldName => StructField( fieldName, StringType, true)))

  val populationRowRDD = populationCSV.map(_.split(";")).map( e => Row(e(0), e(1), e(2), e(3), e(4)))

  val populationDataFrame = sqlContext.createDataFrame(populationRowRDD, populationSchemaType)

  populationDataFrame.registerTempTable("population")

  sqlContext.sql("select * from population").show()

  /*AUTOMATICALLY DEDUCE SCHEMA---- DIRTY HACK*/
  val estatheTxt = sc.textFile("/bigdata/dataset/real_estate/real_estate_transactions.txt")
  //val estatheSchema = "street city zip state beds buffs sq__ft type price"
  val estatheSchema = estatheTxt.first()
  val estatheSchemaType = StructType(estatheSchema.split(";").map(fieldName => StructField( fieldName, StringType, true)))

  val estatheRDD = estatheTxt.filter(row => !(row equals estatheSchema)).map(_.split(";")).map( e => Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8)))
  val estatheDF = sqlContext.createDataFrame(estatheRDD, estatheSchemaType)

  estatheDF.registerTempTable("estathe")

  sqlContext.sql("select * from estathe").show()


  /*PARQUET /bigdata/dataset/userdata*/

  val userDataParquetDF = sqlContext.read.load("/bigdata/dataset/userdata/userdata.parquet")

  //val userDataParquetDF =  sqlContext.sql("SELECT * FROM parquet.`/bigdata/dataset/userdata/userdata.parquet`")


}
