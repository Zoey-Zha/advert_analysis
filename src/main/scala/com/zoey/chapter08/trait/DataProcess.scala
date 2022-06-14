package com.zoey.chapter08.`trait`

import org.apache.spark.sql.SparkSession

trait DataProcess {
  def handler(spark: SparkSession )
}
