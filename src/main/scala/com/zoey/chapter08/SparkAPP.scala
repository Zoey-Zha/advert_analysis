package com.zoey.chapter08

import com.zoey.chapter08.business.{AreaProcessor, LogETLProcessor, ProvinceStatProcessor}
import org.apache.spark.sql.SparkSession

object SparkAPP {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkAPP").master("local[2]").getOrCreate()

    // 先对logs etl
    // LogETLProcessor.handler(spark)

    // get province city stat table
    // ProvinceStatProcessor.handler(spark)

    AreaProcessor.handler(spark)

    spark.stop()
  }

}
