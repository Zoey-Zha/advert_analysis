package com.zoey.chapter04

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SQLContextApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("SQLContextApp")

    val sc = new SparkContext(sparkConf)

    val sc1 = new SQLContext(sc)

    val df = sc1.read.text("file:///Users/xinzha/Documents/02 Projects/advert_analysis/data/input.txt")

    // df.printSchema()
    df.show(false)

    sc.stop()

  }

}
