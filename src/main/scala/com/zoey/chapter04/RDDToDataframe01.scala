package com.zoey.chapter04

import com.zoey.chapter04.DataSetApp.Person
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object RDDToDataframe01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("RDDToDataframe01").getOrCreate()

    runInferSchema(spark)
    spark.stop()
  }

  /**
   * 第一种方式
   * 1. 定义case class
   * 2. RDD to Dataframe时map加上case class
   * @param spark
   */
  private def runInferSchema(spark: SparkSession) = {
    val peopleRDD: RDD[String] = spark.sparkContext.textFile("/Users/xinzha/Documents/02 Projects/advert_analysis/data/people.txt")

    import spark.implicits._

    // TODO -> RDD to DataFrame
    val peopleDF: DataFrame = peopleRDD.map(_.split(","))
      .map(x => Person(x(0), x(1).trim.toInt)) //这个地方用了Person也没多大用吧
      .toDF()

    //    peopleDF.printSchema()
    //    peopleDF.select("name").show()

    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()

    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
  }

  case class Person(name: String, age: Int)

}
