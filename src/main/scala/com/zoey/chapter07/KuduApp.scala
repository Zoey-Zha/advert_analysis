package com.zoey.chapter07

import org.apache.derby.impl.sql.compile.TableName
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.kudu.client.{CreateTableOptions, Insert, KuduClient, KuduScanner, KuduSession, KuduTable, PartialRow, RowResult, RowResultIterator, Update}

import java.util
import scala.collection.JavaConverters._
import collection.mutable._

object KuduApp {

  def main(args: Array[String]): Unit = {
    val MASTER_ADDRESS: String = "hadoop000"
    val client: KuduClient = new KuduClient.KuduClientBuilder(MASTER_ADDRESS)
      .build()

    val tableName = "test"

    // 创建表
    // createTable(client, tableName)

    // 删除表
    deleteTable(client, "ods")
    deleteTable(client,"province_city_stat")

    // 插入数据
    //  inseartRows(client, tableName)

//    queryTable(client, tableName)
//    print("+++++++++++++++++++++++")
//
//    updateTable(client, tableName)
//
//    print("+++++++++++++++++++++++")
//
//    queryTable(client, tableName)

    client.close()

  }

  def createTable(client: KuduClient, tableName: String): Unit = {

    // 下面这个是Scala
    // val scalaList: java.util.List[Int] = List(1,2,3).asJava

    import org.apache.kudu.ColumnSchema
    val columns = new util.ArrayList[ColumnSchema](2)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("word", Type.STRING).key(true).build)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("cnt", Type.INT32).build)

//    val columns: java.util.List[ColumnSchema] = List(
//      new ColumnSchemaBuilder("word", Type.STRING).build(),
//      new ColumnSchemaBuilder("cnt", Type.INT16).build()
//    ).asJava
    // val columns = java

    // Schema的构造器是Java,
    // columns: List[ColumnSchema] 这个是scala, schama用到的是Java,所以需要转换以下
    val schema = new Schema(columns)

    val tableOptions = new CreateTableOptions()
    tableOptions.setNumReplicas(1)
    val list = new util.ArrayList[String]()
    list.add(0, "word")
    tableOptions.addHashPartitions(list,3)

    client.createTable(tableName, schema, tableOptions)
  }

  def deleteTable(client: KuduClient, tableName: String) = {

      client.deleteTable(tableName)
  }


  def inseartRows(client: KuduClient, tableName: String) = {
    val table: KuduTable = client.openTable(tableName)
    val session: KuduSession = client.newSession()

    for (i <- 1 to 10) {
      val insert: Insert = table.newInsert()
      val row: PartialRow = insert.getRow
      row.addString("word", s"alix-$i")
      row.addInt("cnt",100+i)

      session.apply(insert)
    }
  }


  def updateTable(client: KuduClient, tableName: String) = {
    val table: KuduTable = client.openTable(tableName)
    val session: KuduSession = client.newSession()

    val update: Update = table.newUpdate()
    val row: PartialRow = update.getRow()
    row.addString("word", "alix-8")
    row.addInt("cnt",10086)

    session.apply(update)
  }

  def queryTable(client: KuduClient, tableName: String) ={
    val table: KuduTable = client.openTable(tableName)
    val scanner: KuduScanner = client.newScannerBuilder(table).build()

    while(scanner.hasMoreRows()) {
      val iterator: RowResultIterator = scanner.nextRows()
      while(iterator.hasNext) {
        val result: RowResult = iterator.next()
        println(result.getString("word") + "=>" + result.getInt("cnt"))
      }
    }
  }



}
