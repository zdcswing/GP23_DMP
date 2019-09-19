package com.analysis.tagging

import com.utils.Tag
import org.apache.spark.sql.Row

/**
  *
  * @author: ZDC
  * @date: 2019/09/19 11:25
  * @description:
  * @since 1.0
  */
object BusinessTag extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    var row = args(0).asInstanceOf[Row]

    if (row.getAs[String]("long").toDouble >= 73
      && row.getAs[String]("long").toDouble <= 136
      && row.getAs[String]("lat").toDouble >= 3
      && row.getAs[String]("lat").toDouble <= 53){
      val long = row.getAs[String]("long").toDouble
      val lat = row.getAs[String]("lat").toDouble
    }
      list
  }
}
