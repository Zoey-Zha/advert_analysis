package com.zoey.chapter04

import org.apache.spark.sql.SparkSession

object SparkSessionApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkSessionApp")
      .getOrCreate()

    val df = spark.read.text("file:///Users/xinzha/Documents/02 Projects/advert_analysis/data/people.txt")

    // df.show()
    df.printSchema()

    // TODO 进行其他业务处理

    spark.stop()

  }

}
