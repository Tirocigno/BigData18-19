package casestudy

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

object CaseStudyMain {

  /*JSON LOADING*/
  val sc = new SparkContext()
  val sqlContext = new SQLContext(sc)

  /*Reading from Oracle DB*/
  val url = "jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF"
  val attributesTable = "CRM_POS_ATTRIBUTES"
  val potentialTable = "CRM_POS_POTENTIAL"
  val attributesDF = sqlContext.read.format("jdbc").options(Map("driver"->"oracle.jdbc.driver.OracleDriver","url" -> url,"dbtable" -> attributesTable)).load()
  val potentialDF = sqlContext.read.format("jdbc").options(Map("driver"->"oracle.jdbc.driver.OracleDriver","url" -> url,"dbtable" -> potentialTable)).load()

  /*Hive interaction is REALLY EASY*/
  val ppeTable = "bigdata_group66.ppe"
  val ppeDF = sqlContext.sql(s"select * from $ppeTable")

  val germanyFilePath = "/bigdata/dataset/postcodes/geo_germany.txt"
  val germanyGeographySchemaText = "id land landkreis city zipcode"
  val germanyPopulationSourceFile = sc.textFile(germanyFilePath)

  val germanySchema = StructType(germanyGeographySchemaText.split(" ").map(fieldName => StructField( fieldName, StringType, true)))

  val populationRowRDD = germanyPopulationSourceFile.map(_.split(",")).map( e => Row(e(0), e(1), e(2), e(3), e(4)))

  val germanyPopulationDF = sqlContext.createDataFrame(populationRowRDD, germanySchema)

  val filteredAttributesDF = attributesDF.filter("flag = 'Keep'")
  filteredAttributesDF.registerTempTable("OnlyKeepCRM_POS_ATTRIBUTES")


}
