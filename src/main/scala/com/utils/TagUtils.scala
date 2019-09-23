package com.utils

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

import scala.tools.scalap.scalax.util.StringUtil

/**
  *
  * @author: ZDC
  * @date: 2019/09/19 9:55
  * @description:
  * @since 1.0
  */
object TagUtils {
  def getallUserId(row: Row) = {
    var list = List[String]()
    if (StringUtils.isNotBlank(row.getAs[String]("imei"))) list :+= "IM" + row.getAs[String]("imei")
    if (StringUtils.isNotBlank(row.getAs[String]("mac"))) list :+= "MC" + row.getAs[String]("mac")
    if (StringUtils.isNotBlank(row.getAs[String]("idfa"))) list :+= "ID" + row.getAs[String]("idfa")
    if (StringUtils.isNotBlank(row.getAs[String]("openudid"))) list :+= "OD" + row.getAs[String]("openudid")
    if (StringUtils.isNotBlank(row.getAs[String]("androidid"))) list :+= "AD" + row.getAs[String]("androidid")
    if (StringUtils.isNotBlank(row.getAs[String]("imeimd5"))) list :+= "IM" + row.getAs[String]("imeimd5")
    if (StringUtils.isNotBlank(row.getAs[String]("macmd5"))) list :+= "MC" + row.getAs[String]("macmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("idfamd5"))) list :+= "ID" + row.getAs[String]("idfamd5")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidmd5"))) list :+= "OD" + row.getAs[String]("openudidmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("androididmd5"))) list :+= "AD" + row.getAs[String]("androididmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("imeisha1"))) list :+= "IM" + row.getAs[String]("imeisha1")
    if (StringUtils.isNotBlank(row.getAs[String]("macsha1"))) list :+= "MC" + row.getAs[String]("macsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("idfasha1"))) list :+= "ID" + row.getAs[String]("idfasha1")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidsha1"))) list :+= "OD" + row.getAs[String]("openudidsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("androididsha1"))) list :+= "AD" + row.getAs[String]("androididsha1")
    list
  }


  def getOneUserId(row: Row): String = {
    row match {
      case it if StringUtils.isNotBlank(it.getAs[String]("imei")) => "IM" + it.getAs[String]("imei")
      case it if StringUtils.isNotBlank(it.getAs[String]("mac")) => "MC" + it.getAs[String]("mac")
      case it if StringUtils.isNotBlank(it.getAs[String]("idfa")) => "ID" + it.getAs[String]("idfa")
      case it if StringUtils.isNotBlank(it.getAs[String]("openudid")) => "OD" + it.getAs[String]("openudid")
      case it if StringUtils.isNotBlank(it.getAs[String]("androidid")) => "AD" + it.getAs[String]("androidid")

      case it if StringUtils.isNotBlank(it.getAs[String]("imeimd5")) => "IM" + it.getAs[String]("imeimd5")
      case it if StringUtils.isNotBlank(it.getAs[String]("macmd5")) => "MC" + it.getAs[String]("macmd5")
      case it if StringUtils.isNotBlank(it.getAs[String]("idfamd5")) => "ID" + it.getAs[String]("idfamd5")
      case it if StringUtils.isNotBlank(it.getAs[String]("openudidmd5")) => "OD" + it.getAs[String]("openudidmd5")
      case it if StringUtils.isNotBlank(it.getAs[String]("androididmd5")) => "AD" + it.getAs[String]("androididmd5")

      case it if StringUtils.isNotBlank(it.getAs[String]("imeisha1")) => "IM" + it.getAs[String]("imeisha1")
      case it if StringUtils.isNotBlank(it.getAs[String]("macsha1")) => "MC" + it.getAs[String]("macsha1")
      case it if StringUtils.isNotBlank(it.getAs[String]("idfasha1")) => "ID" + it.getAs[String]("idfasha1")
      case it if StringUtils.isNotBlank(it.getAs[String]("openudidsha1")) => "OD" + it.getAs[String]("openudidsha1")
      case it if StringUtils.isNotBlank(it.getAs[String]("androididsha1")) => "AD" + it.getAs[String]("androididsha1")
      case _ => "其他"


    }
  }
}
