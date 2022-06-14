package com.zoey.chapter08.business

import com.zoey.Utils.{KuduUtil, SQLUtil, SchemaUtil}
import org.apache.kudu.client.KuduClient
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProvinceStatApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[2]")
      .appName("ProvinceStatApp")
      .getOrCreate()

    // 从kudu读ods数据 并注册成表
    val master = "hadoop000"
    val odsDF: DataFrame = spark.read.format("org.apache.kudu.spark.kudu").option("kudu.table", "ods")
      .option("kudu.master", master).load()

    odsDF.createOrReplaceTempView("ods")
    // val resDF: DataFrame = spark.sql(SQLUtil.PROVINCE_STAT_SQL)
    val resDF: DataFrame = spark.sql(SQLUtil.TEST_SQL)

    val tableName = "province_city_stat"
    val partitionId = "provincename"

    KuduUtil.kuduSink(master, tableName, SchemaUtil.ProvinceSchema, partitionId,resDF)

    spark.stop()

  }

}
