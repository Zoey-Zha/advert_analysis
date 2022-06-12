package com.zoey.chapter07

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.kudu.client.{CreateTableOptions, KuduClient}

import java.util
import scala.collection.JavaConverters._
import collection.mutable._

object KuduApp {


  def main(args: Array[String]): Unit = {
    val MASTER_ADDRESS: String = "hadoop000"
    val client: KuduClient = new KuduClient.KuduClientBuilder(MASTER_ADDRESS)
      .build()

    val tableName = "test"

//    val scalaList: java.util.List[Int] = List(1,2,3).asJava
//    val jul: java.util.List[Int] = List(1, 2, 3).asJava



    createTable(client, tableName)

    client.close()

  }

  def createTable(client: KuduClient, tableName: String): Unit = {

    // 下面这个是Scala
    // val scalaList: java.util.List[Int] = List(1,2,3).asJava

    import org.apache.kudu.ColumnSchema
    val columns = new util.ArrayList[ColumnSchema](2)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("word", Type.STRING).key(true).build)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("cnt", Type.INT16).build)

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


}
