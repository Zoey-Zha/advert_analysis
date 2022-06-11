package com.zoey.chapter04

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object RDDToDataframe02 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("RDDToDataframe02").getOrCreate()

    import spark.implicits._
    val peopleRDD: RDD[String] = spark.sparkContext.textFile("/Users/xinzha/Documents/02 Projects/advert_analysis/data/people.txt")

    // 从这里看，RDD可以直接转为dataframe, 那么回到问题：为什么要大费周折搞structype呢
    // 因为要把RDD转为DataSet
    val dataFrame: Dataset[String] = peopleRDD.toDS()
    val value: Dataset[String] = peopleRDD.toDS()

    // 可以看到spilt后的数据类型是String类型的数组
    val tempRDD: RDD[Array[String]] = peopleRDD.map(_.split(","))

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    // 不太懂这种用法
    val fields: Array[StructField] = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    // fields.foreach(println)
    val schema = StructType(fields)



    // second way to create a StructTpye
    val structType: StructType = StructType(Array(StructField("name", StringType, true), StructField("age", StringType, true)))

    // Thrid way to create a StructType
    val struct =
      StructType(
        StructField("name", StringType, true) ::
          StructField("age", StringType, false) ::Nil)

    // Convert records of the RDD (people) to Rows
    val rowRDD: RDD[Row] = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))


    // Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT name FROM people")

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    results.map(attributes => "Name: " + attributes(0)).show()


    spark.stop()
  }

}
