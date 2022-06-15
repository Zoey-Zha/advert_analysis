package com.zoey.Utils

import org.apache.spark.sql.SparkSession

object DateUtils {
  def getTableName(tableName: String, spark: SparkSession): String = {
    val date: String = spark.sparkContext.getConf.get("spark.date")
    //val testDate = "20220614"
    val newTableName = tableName + "_" + date
    newTableName
  }


}
