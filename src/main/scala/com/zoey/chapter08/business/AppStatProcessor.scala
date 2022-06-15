package com.zoey.chapter08.business

import com.zoey.Utils.{DateUtils, KuduUtil, SQLUtil, SchemaUtil}
import com.zoey.chapter08.`trait`.DataProcess
import org.apache.spark.sql.{DataFrame, SparkSession}

object AppStatProcessor extends DataProcess{
  override def handler(spark: SparkSession): Unit = {
    val master = "hadoop000"
    val sourceTable = DateUtils.getTableName("ods", spark)
    val odsDF: DataFrame = spark.read.format("org.apache.kudu.spark.kudu").option("kudu.table", sourceTable)
      .option("kudu.master", master).load()

    odsDF.createOrReplaceTempView("ods")

    val areaDF: DataFrame = spark.sql(SQLUtil.APP_SQL_STEP1)
    areaDF.createOrReplaceTempView("app_tmp")

    val resDF: DataFrame = spark.sql(SQLUtil.APP_SQL_STEP2)
    // resDF.show(false)

//    val tableName = "app_stat"
    val tableName = DateUtils.getTableName("app_stat", spark)
    val partitionId = "appid"

    KuduUtil.kuduSink(master, tableName, SchemaUtil.APPSchema, partitionId,resDF)
  }
}
