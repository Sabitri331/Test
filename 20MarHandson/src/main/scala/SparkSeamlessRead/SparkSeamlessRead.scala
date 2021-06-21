package SparkSeamlessRead

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.databricks.spark.xml
import org.apache.spark.sql.functions._
import scala.io.Source
object Task1 {
case class schema(txnno:String, txndate:String,custno:String, amount:String,category:String, product:String,city:String, state:String, spendby:String)
def main(args:Array[String]):Unit={

		println("Hello World")

		val conf = new SparkConf().setAppName("first").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")

		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._

		//		val jsoncsvdf = spark.read.format("json").option("multiline",true).load("file:///C:/data/zeyo27_json.json")
		//		jsoncsvdf.show()
		//		jsoncsvdf.printSchema()
		//
		//		val explodjsondf = jsoncsvdf.select(
		//				col("address.permanent_address"),
		//				col("address.temporary_address"),
		//				col("*")
		//				).withColumn("students",explode(col("students")))
		//		explodjsondf.show()
		//
		//		val flatfile  = explodjsondf.select(
		//				col("*"),
		//				col("students.location"),
		//				col("students.name"))
		//		.drop("address","students")
		//		flatfile.show(false)
		//		flatfile.printSchema()
		//
		//		println("----Back to Original----")
		//		
		//		val origfile = flatfile.select(
		//				struct(
		//						col("permanent_address"),
		//						col("temporary_address")
		//						).alias("address"),
		//				col("*"))
		//
		//		val origfile1 = origfile.groupBy(
		//				"address",
		//				"first_name",
		//				"second_name",
		//				"trainer").agg(collect_list(
		//						struct(
		//								col("location"),
		//								col("name")
		//								)).alias("students"))
		//
		//		origfile1.show(false)
		//		origfile1.printSchema()
		//
		//		origfile1.write.format("json").mode("overwrite").save("file:///D:/data/jsonfile")

		println("------Task2-------------------")
		val url = Source.fromURL("https://randomuser.me/api/0.8/?results=5")
		val strdata = url.mkString
		println(strdata)
		val strrdd = sc.parallelize(List(strdata))
		println(strrdd)
		val jsondf = spark.read.json(strrdd)
		jsondf.show()
		jsondf.printSchema()

		val flaturl = jsondf.withColumn("results",explode(col("results")))
		.select("*","results.user.*","results.user.location.*","results.user.name.*","results.user.picture.*").drop("results","location","name","picture")

		flaturl.show()

}
}