package com.zoey.chapter04

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat}

object DataFrameApiApp1 {
  def main(args: Array[String]): Unit = {
    // SparkSession后没有(), 这里也不用new
    lazy val sparkSession = SparkSession.builder().master("local")
      .appName("DataFrameApiApp").getOrCreate()

    val peopleDF = sparkSession.read.json("file:///Users/xinzha/Documents/02 Projects/advert_analysis/data/people.json")

    // peopleDF.printSchema();
   //  import spark.implicits._

    peopleDF.select("name").show()

    // here why not import spark.implicits._
    // There is no sparl.implicits._
    import sparkSession.implicits._

    // peopleDF.select($"name")
    // peopleDF.select('name)
    // peopleDF.select(col("name"))
    //  peopleDF.selectExpr("name",)

    sparkSession.stop()

  }

}
