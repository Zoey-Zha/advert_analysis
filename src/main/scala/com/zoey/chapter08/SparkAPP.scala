package com.zoey.chapter08

import com.zoey.chapter08.business.{AppStatProcessor, AreaProcessor, LogETLProcessor, ProvinceStatProcessor}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object SparkAPP extends Logging{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()

    // 提交的时候需要加上 --conf date=20220614
    //      .appName("SparkAPP").master("local[2]")
    //      .config("spark.date","20220614")
    //      .config("spark.raw.path", "file:///Users/xinzha/Documents/02 Projects/advert_analysis/data/data-test.json")
    //      .config("spark.ip.path", "file:///Users/xinzha/Documents/02 Projects/advert_analysis/data/ip.txt")
    //      .getOrCreate()
    /**
     * spark-submit需要三个参数
     * 1. spark.date
     * 2. spark.raw.path
     * 3. spark.ip.path
     */
    val date: String = spark.sparkContext.getConf.get("spark.date")
    // 判断参数是否为空
    if(date == null) {
      logError("时间参数为空")
      System.exit(0)
    }

     // 先对logs etl
    LogETLProcessor.handler(spark)

     // get province city stat table
    ProvinceStatProcessor.handler(spark)

    // area stat
    AreaProcessor.handler(spark)

     // app data stat
    AppStatProcessor.handler(spark)

    spark.stop()
  }

}
