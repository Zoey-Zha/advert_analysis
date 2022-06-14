package com.zoey.Utils

import org.apache.kudu.Schema
import org.apache.kudu.client.{CreateTableOptions, KuduClient}
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.spark.sql.DataFrame

import java.util.LinkedList

object KuduUtil {
  def kuduSink(master: String, tableName: String, schema: Schema,partitionId: String, df: DataFrame): Unit = {
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
    client.close()
  }

}
