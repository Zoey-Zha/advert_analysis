package com.zoey.chapter08

import com.zoey.Utils.{IPUtils, SQLUtil, SchemaUtil}
import org.apache.kudu.client.{CreateTableOptions, KuduClient}
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util
import java.util.LinkedList

object LogETLApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[2]")
      .appName("LogETLAPP")
      .getOrCreate()

    val jsonDF: DataFrame = spark.read.json("file:///Users/xinzha/Documents/02 Projects/advert_analysis/data/data-test.json")

    val ipRuleRDD: RDD[String] = spark.sparkContext.textFile("file:///Users/xinzha/Documents/02 Projects/advert_analysis/data/ip.txt")

    import spark.implicits._

    val ipRuleDF: DataFrame = ipRuleRDD.map(x => {
      val splits: Array[String] = x.split("\\|")
      val startIP: Long = splits(2).toLong
      val endIP: Long = splits(3).toLong
      // val country: String = splits(5)
      val province: String = splits(6)
      val city: String = splits(7)
      val isp: String = splits(9)
      (startIP, endIP, province, city, isp) // 返回字段
    }).toDF("start_ip", "end_ip", "province", "city", "isp") // 这个貌似也是必须的
    // 写到表里的数据，单词之间用下划线

    import org.apache.spark.sql._

    // first way to have a udf
    val iPUtil: UserDefinedFunction = spark.udf.register("IPUtil", IPUtils.ip2Long _)

    // second way to have a udf
    val getIP = (s: String) => {
      IPUtils.ip2Long(s)
    }

    val getLongIp: UserDefinedFunction = udf(getIP)

    // val newJsonDF = jsonDF.withColumn("ipLong",iPUtil('ip)).select('ipLong)
    // add new column by withColumn
    val newJsonDF = jsonDF.withColumn("ip_long",getLongIp('ip))

    newJsonDF.createOrReplaceTempView("logs")
    ipRuleDF.createOrReplaceTempView("ips")
    val sql = SQLUtil.SQL
    val resDF: DataFrame = spark.sql(sql)

    // val resDF: DataFrame = newJsonDF.join(ipRuleDF, newJsonDF("ip_long").between(ipRuleDF("startIP"), ipRuleDF("endIP")), "left")
    /*
    root
    |-- ipLong: long (nullable = false)
    |-- startIP: long (nullable = true)
    |-- endIP: long (nullable = true)
    |-- province: string (nullable = true)
    |-- city: string (nullable = true)
    |-- isp: string (nullable = true)
    */

    val master = "hadoop000"
    val partitionId = "ip"
    val tableName = "ods"
    val schema = SchemaUtil.ODSSchema
    val client: KuduClient = new KuduClientBuilder(master).build()

    val options: CreateTableOptions = new CreateTableOptions()
    options.setNumReplicas(1)
    // val parcols: util.LinkedList[String] = new util.LinkedList[String]()
    // 不加util???
    val parcols: LinkedList[String] = new LinkedList[String]()

    parcols.add(partitionId)

    options.addHashPartitions(parcols,3)

    // 创建表
    if(client.tableExists(tableName)) {
      client.deleteTable(tableName)
    }
    client.createTable(tableName,schema,options)


    resDF.write.mode(SaveMode.Append) //
      .format("org.apache.kudu.spark.kudu") //
      .option("kudu.table",tableName)
      .option("kudu.master", master)
      .save()


    spark.stop()
  }

}
