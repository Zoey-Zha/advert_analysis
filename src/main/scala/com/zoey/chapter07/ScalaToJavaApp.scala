package com.zoey.chapter07
import scala.collection.JavaConverters._
import collection.mutable._

object ScalaToJavaApp {
  def main(args: Array[String]): Unit = {
//    val scalaList = List(1,2,3)
//    scalaList.asJava

    val jul: java.util.List[Int] = List(1, 2, 3).asJava

  }

}
