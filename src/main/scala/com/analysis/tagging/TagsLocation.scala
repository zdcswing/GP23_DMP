package com.analysis.tagging

import com.utils.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  *
  * @author: ZDC
  * @date: 2019/09/22 15:41
  * @description:
  * @since 1.0
  */
object TagsLocation extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row: Row = args(0).asInstanceOf[Row]

    val pro: String = row.getAs[String]("provincename")
    val city: String = row.getAs[String]("cityname")

    if(StringUtils.isNotBlank(pro)){
      list :+= ("ZP" + pro, 1)
    }

    if(StringUtils.isNotBlank(city)){
      list :+= ("ZC" + city, 1)
    }
    list
  }
}
