package casestudy.cloud


import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

class CaseStudyCloud {

  val sc = new SparkContext()
  val sqlContext = new SQLContext(sc)


  def storeTable(df:DataFrame, dfName:String) = {
    df.write.option("path", "s3://fnaldini-bigdata-unibo/staged_area/")
      .mode("append")
      .saveAsTable("geomarketing."+ dfName)
  }

  var posAttrDF = sqlContext.sql("select * from geomarketing.pos_attributes")
  posAttrDF.registerTempTable("pos_attr")
  // we can consider at the beginning just the ones to "Keep"
  posAttrDF = sqlContext.sql("select * from pos_attr where flag = 'Keep' ")
  posAttrDF.registerTempTable("pos_attr")

  val posPotDF = sqlContext.sql("select * from geomarketing.pos_potential")
  posPotDF.registerTempTable("pos_pot")




  // Create a dataframe from Hive Table
  val ppeDF = sqlContext.sql("select * from geomarketing.ppe")
  ppeDF.registerTempTable("ppe")

  //Load geography Germany from Text file, infering the schema


  // Create Geography dataframe
  val geoGermanyDF = sqlContext.sql("select * from geomarketing.geogermany")
  geoGermanyDF.registerTempTable("geography")


  // Calculate the total potential for each zipcode
  val landkreisPotentialDF = sqlContext.sql("select landkreis, sum(vehicle_qty) vehicle_qty,sum(potential) tot_potential from ppe x join geography y on x.zipcode = y.zipcode group by landkreis")

  landkreisPotentialDF.registerTempTable("total_pot_landkreis")

  storeTable(landkreisPotentialDF, "landkreisPotential")


  // Calculate PoS' potential for each zipcode
  val posPotAllocatedDF = sqlContext.sql("select landkreis,sum(potential) alloc_potential from pos_pot a join pos_attr b on a.pos_id = b.pos_id join geography c on b.zipcode = c.zipcode where flag = 'Keep' group by landkreis")
  posPotAllocatedDF.registerTempTable("tot_pot_allocated")

  storeTable(posPotAllocatedDF, "allocated_potential_landkreis")


  // Calculate the potential to allocate, per zipcode
  val potToAllocateDF = sqlContext.sql("select a.landkreis,sum(a.tot_potential-b.alloc_potential) potential_left from total_pot_landkreis a left join tot_pot_allocated b on a.landkreis = b.landkreis  group by a.landkreis")
  potToAllocateDF.registerTempTable("potential_to_allocate_landkreis")

  //potToAllocateDF.saveAsTable("potential_to_allocate_landkreis")
  storeTable(potToAllocateDF,"potential_to_allocate_landkreis")

  // Calculate average by subtypology
  val avgSubtypologyDF = sqlContext.sql("select landkreis,subtypology,sum(potential) / count(a.pos_id) subtypology_avg from pos_pot a join pos_attr b on a.pos_id = b.pos_id join geography c on b.zipcode = c.zipcode where flag = 'Keep' group by landkreis,subtypology order by 1,2")

  avgSubtypologyDF.registerTempTable("avgSubtypology")


  val totAvgSubtypologyDF = sqlContext.sql("select landkreis,sum(subtypology_avg) totAvgSubtypology from avgSubtypology group by landkreis order by 1")

  totAvgSubtypologyDF.registerTempTable("totAvgSubtypology")
  val weightDF = sqlContext.sql("select x.landkreis,subtypology,(subtypology_avg/totAvgSubtypology) weightLandkreis  from avgSubtypology x join totAvgSubtypology y on x.landkreis = y.landkreis ")

  weightDF.registerTempTable("weight")
  //weightDF.saveAsTable("weight_landkreis_sub")
  storeTable(weightDF,"weight_landkreis_sub" )

  val potentialAllocationDF = sqlContext.sql("select  pos_code,g.landkreis,a.subtypology,case when potential = 0 then weightLandkreis*potential_left else potential end as final_potential from  pos_attr a join pos_pot b on a.pos_id = b.pos_id join geography g on a.zipcode = g.zipcode join weight w on g.landkreis = w.landkreis and a.subtypology = w.subtypology join potential_to_allocate_landkreis p on g.landkreis = p.landkreis  where flag = 'Keep' order by 2,1")
 //c potentialAllocationDF.saveAsTable("final_pos_potential")
  storeTable(potentialAllocationDF,"final_pos_potential" )

}
