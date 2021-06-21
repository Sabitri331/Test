package SparkSeamlessRead

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import com.databricks.spark.xml
import org.apache.spark.sql.functions._
import scala.io.Source
import com.mysql.jdbc.Driver
import org.apache.hadoop.fs.s3a._
//import org.apache.spark.sql.DataFrame
object Task1 {
case class schema(txnno:String, txndate:String,custno:String, amount:String,category:String, product:String,city:String, state:String, spendby:String)
def main(args:Array[String]):Unit={

		println("Hello World")

		val conf = new SparkConf().setAppName("first").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")

		val spark = SparkSession.builder()
		.config("fs.s3a.access.key","AKIAQJ35YRX5KZCWFNDI")
		.config("fs.s3a.secret.key","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")
		.getOrCreate()
		import spark.implicits._
		
		println("Task1 Started")  
val df1 = spark.read.format("csv").option("header","true").load("file:///C:/data/subtract.csv")
df1.show()
df1.dtypes
df1.createOrReplaceTempView("df_1")
val df2 = spark.sql("select year,sales, NVL((sales - lag(sales,1) over (order by year)),0) tot_sell FROM df_1").show()
////val win_func = Window.partitionBy("country").orderBy("week")
////val df2 = df1.withColumn("lag",lag("sell",1).over(win_func)).show()
//df1.createOrReplaceTempView("df_1")
//val df2 = spark.sql("select country,week,sell, (sell - NVL(lag(sell,1) over (partition by country order by week),0)) tot_sell FROM df_1").show()
}
}