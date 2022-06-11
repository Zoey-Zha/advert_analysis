package com.zoey.chapter04

import org.apache.spark.sql.{Dataset, SparkSession}

object DataSetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local")
      .appName("DataSetApp").getOrCreate()

    import spark.implicits._
    val caseClassDS: Dataset[Person] = Seq(Person("Zoey", 30)).toDS()

    // caseClassDS.show()

    val primitiveDS: Dataset[Int] = Seq(1, 2, 3).toDS()
    // Encoders for most common types are automatically provided by importing spark.implicits._
    primitiveDS.map(_ + 1).collect()
    // .foreach(println)// Returns: Array(2, 3, 4)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "/Users/xinzha/Documents/02 Projects/advert_analysis/data/people.json"
    val peopleDS: Dataset[Person] = spark.read.json(path).as[Person]
    // peopleDS.map(x => x.name).show() // 这个因为文件中有空数据，就不行！？
    // peopleDS.show()
    peopleDS.select("name").show()

    spark.stop()
  }

  // case class Person(name: String, age: Long)
  case class Person(name: String, age: Long)

}
