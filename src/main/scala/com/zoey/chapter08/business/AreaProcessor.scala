package com.zoey.chapter08.business

import com.zoey.Utils.{DateUtils, KuduUtil, SQLUtil, SchemaUtil}
import com.zoey.chapter08.`trait`.DataProcess
import org.apache.spark.sql.{DataFrame, SparkSession}

object AreaProcessor extends DataProcess{
  override def handler(spark: SparkSession): Unit = {
    val master = "hadoop000"
    val sourceTable = DateUtils.getTableName("ods", spark)
    val odsDF: DataFrame = spark.read.format("org.apache.kudu.spark.kudu").option("kudu.table", sourceTable)
      .option("kudu.master", master).load()

    odsDF.createOrReplaceTempView("ods")

    val areaDF: DataFrame = spark.sql(SQLUtil.AREA_SQL_STEP1)
    areaDF.createOrReplaceTempView("area_tmp")

    val resDF: DataFrame = spark.sql(SQLUtil.AREA_SQL_STEP2)
    // resDF.show(false)

    // val tableName = "area_stat"
    val tableName = DateUtils.getTableName("area_stat", spark)
    val partitionId = "provincename"

    KuduUtil.kuduSink(master, tableName, SchemaUtil.AREASchema, partitionId,resDF)
  }
}
