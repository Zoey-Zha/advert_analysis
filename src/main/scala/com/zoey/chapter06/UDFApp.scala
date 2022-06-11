package com.zoey.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object UDFApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local")
      .appName("UDFApp").getOrCreate()

    val hobbyRDD: RDD[String] = spark.sparkContext.textFile("/Users/xinzha/Documents/02 Projects/advert_analysis/data/hobbies.txt")

    import spark.implicits._

    val hobbyDF: DataFrame = hobbyRDD.map(_.split("\t")).map(x => Hobby(x(0), x(1))).toDF()

    // hobbyDF.show(false)
    // 定义函数和注册udf
    spark.udf.register("hobby_size", (s:String) => s.split(",").size)

    // hobbyDF.createTempView("hobbies_zoey")
    hobbyDF.createOrReplaceTempView("hobbies_zoey")

    val udfDF: DataFrame = spark.sql("select name, hobby_size(hobbies) as hobby_num from hobbies_zoey")

    udfDF.show()

    spark.stop()
  }

  case class Hobby(name:String, hobbies: String)

}
