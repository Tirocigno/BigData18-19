////////// Exercise 1: approximating distinct count (HyperLogLog++)
///Necessary only sql to import
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.functions._



val sc = new SparkContext()
//Automatically imported inside spark shell, not create
val sqlContext= new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val a = sc.textFile("hdfs:/bigdata/dataset/tweet").map(_.split("\\|")).filter(_(0)!="LANGUAGE")
val b = a.filter(_(2)!="").flatMap(_(2).split(" ")).filter(_!="").map(_.replace(",","")).toDF("hashtag").cache()
b.collect()


// Exact result
b.agg(countDistinct("hashtag")).collect()
// Approximate result (default relative standard deviation = 0.05)
b.agg(approxCountDistinct("hashtag")).collect()
b.agg(approxCountDistinct("hashtag",0.1)).collect()
b.agg(approxCountDistinct("hashtag",0.01)).collect()


////////// Exercise 2: approximating membership (Bloom filter)

// Exact result
b.filter("hashtag = '#vaccino'").limit(1).count() // returns 1
b.filter("hashtag = '#vaccino2'").limit(1).count() // returns 0

// bloomFilter triggers an action; n=1000, p=0.01
val bf = b.stat.bloomFilter("hashtag", 1000, 0.01)
bf.mightContain("#vaccino") // returns true
bf.mightContain("#vaccino2") // probably returns false

val bf1 = b.stat.bloomFilter("hashtag", 1000, 10)
bf1.mightContain("#vaccino") // returns true
bf1.mightContain("#vaccino2") // may return true


////////// Exercise 3: approximating frequency (Count-Min sketch)

//Exact result
b.filter("hashtag = '#vaccino'").count()

// countMinSketch triggers an action; ε=0.01, 1-δ=0.99, seed=10
val cms = b.stat.countMinSketch("hashtag",0.01,0.99,10)
cms.estimateCount("#vaccino")

val cms1 = b.stat.countMinSketch("hashtag",0.1,0.99,10)
cms1.estimateCount("#vaccino")

val cms2 = b.stat.countMinSketch("hashtag",0.01,0.9,10)
cms2.estimateCount("#vaccino")


