package com.zoey.chapter08.business

import com.zoey.Utils.{KuduUtil, SQLUtil, SchemaUtil}
import com.zoey.chapter08.`trait`.DataProcess
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProvinceStatProcessor extends DataProcess{
  override def handler(spark: SparkSession): Unit = {
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
  }
}
