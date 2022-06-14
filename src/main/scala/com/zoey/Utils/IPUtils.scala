package com.zoey.Utils

object IPUtils {

  def ip2Long (ip: String): Long = {
    // dot needs to escape
    val spilts: Array[String] = ip.split("\\.")
    var numOfIP: Long = 0L
    // 这样写不对啊？？
//    for (i <- 0 to (spilts.size-1)) {
//      numOfIP = spilts(i).toLong + (numOfIP << 3)
//    }

    for (i <- 0 until(spilts.length)) {
      numOfIP = spilts(i).toLong | numOfIP << 8L
    }
    numOfIP
  }

}
