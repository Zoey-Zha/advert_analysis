package com.zoey.chapter01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkWordCountApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("SparkWordCountApp")
    // 这里为什么要申明context呢
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("file:///Users/xinzha/Documents/02 Projects/advert_analysis/data/input.txt")

    rdd.flatMap(_.split(",")).map(word=>(word,1))
      .reduceByKey(_+_)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
    .collect().foreach(println)
      // .saveAsTextFile("file:///Users/xinzha/Documents/02 Projects/advert_analysis/out")



    // rdd.collect().foreach(println)

    sc.stop()

  }

}
