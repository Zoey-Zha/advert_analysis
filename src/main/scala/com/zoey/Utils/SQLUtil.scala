package com.zoey.Utils

object SQLUtil {
  lazy val SQL = "select " +
    "logs.ip ," +
    "logs.sessionid," +
    "logs.advertisersid," +
    "logs.adorderid," +
    "logs.adcreativeid," +
    "logs.adplatformproviderid" +
    ",logs.sdkversion" +
    ",logs.adplatformkey" +
    ",logs.putinmodeltype" +
    ",logs.requestmode" +
    ",logs.adprice" +
    ",logs.adppprice" +
    ",logs.requestdate" +
    ",logs.appid" +
    ",logs.appname" +
    ",logs.uuid, logs.device, logs.client, logs.osversion, logs.density, logs.pw, logs.ph" +
    ",ips.province as provincename" +
    ",ips.city as cityname" +
    ",ips.isp as isp" +
    ",logs.ispid, logs.ispname" +
    ",logs.networkmannerid, logs.networkmannername, logs.iseffective, logs.isbilling" +
    ",logs.adspacetype, logs.adspacetypename, logs.devicetype, logs.processnode, logs.apptype" +
    ",logs.district, logs.paymode, logs.isbid, logs.bidprice, logs.winprice, logs.iswin, logs.cur" +
    ",logs.rate, logs.cnywinprice, logs.imei, logs.mac, logs.idfa, logs.openudid,logs.androidid" +
    ",logs.rtbprovince,logs.rtbcity,logs.rtbdistrict,logs.rtbstreet,logs.storeurl,logs.realip" +
    ",logs.isqualityapp,logs.bidfloor,logs.aw,logs.ah,logs.imeimd5,logs.macmd5,logs.idfamd5" +
    ",logs.openudidmd5,logs.androididmd5,logs.imeisha1,logs.macsha1,logs.idfasha1,logs.openudidsha1" +
    ",logs.androididsha1,logs.uuidunknow,logs.userid,logs.iptype,logs.initbidprice,logs.adpayment" +
    ",logs.agentrate,logs.lomarkrate,logs.adxrate,logs.title,logs.keywords,logs.tagid,logs.callbackdate" +
    ",logs.channelid,logs.mediatype,logs.email,logs.tel,logs.sex,logs.age " +
    "from logs left join " +
    "ips on logs.ip_long between ips.start_ip and ips.end_ip "

}
