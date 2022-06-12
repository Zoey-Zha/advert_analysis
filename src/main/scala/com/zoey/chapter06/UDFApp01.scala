package com.zoey.chapter06

import com.zoey.chapter06.UDFApp.Hobby
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object UDFApp01 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local")
      .appName("UDFApp01").getOrCreate()


//    import spark.implicits._
//    val columns = Seq("Seqno","Quote")
//    val data = Seq(("1", "Be the change that you wish to see in the world"),
//      ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
//      ("3", "The purpose of our lives is to be happy.")
//    )
//    val df = data.toDF(columns:_*)
//    df.show(false)


    val convertCase =  (strQuote:String) => {
      val arr: Array[String] = strQuote.split(",")
      arr.size
      // 不太懂这个操作：f 是什么：Array[String]，那么不应该是f(0)吗？
      // 这里map的对象是每一个String?不同于RDD[Array[String]],map的时候是作用于每一行
      // arr.map(f=>  f.substring(0,1).toUpperCase + f.substring(1,f.length)).mkString(" ")
    }

    // 注册成udf
    // val convertUDF = udf(convertCase)
    spark.udf.register("convertUDF",convertCase)


    //Using with DataFrame
//    df.select(col("Seqno"),
//      convertUDF(col("Quote")).as("Quote") ).show(false)

    val hobbyRDD: RDD[String] = spark.sparkContext.textFile("/Users/xinzha/Documents/02 Projects/advert_analysis/data/hobbies.txt")

    import spark.implicits._

    //test map
    val testRDD: RDD[Array[String]] = hobbyRDD.map(_.split("\t"))

    val hobbyDF: DataFrame = hobbyRDD.map(_.split("\t")).map(x => Hobby(x(0), x(1))).toDF("name","hobbies")
    // hobbyDF.select(convertUDF('hobbies).as("hobby_num")).show(false)
    hobbyDF.createOrReplaceTempView("hobbies_view")

    // TODO 上面的UDF不能用于spark.sql吗？
    // This function is neither a registered temporary function nor a permanent function registered in the database 'default'.; line 1 pos 13
    // 使用spark.sql，其中要使用udf时，udf必须要register，否则会报上面的错误
    val res: DataFrame = spark.sql("select name, convertUDF(hobbies) as hobby_num from hobbies_view")

    res.show(false)



    spark.stop()
  }

  case class Hobby(name:  String, hobbies: String)

}
