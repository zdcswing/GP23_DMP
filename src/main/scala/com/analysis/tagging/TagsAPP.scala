package com.analysis.tagging

import com.utils.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  *
  * @author: ZDC
  * @date: 2019/09/22 9:25
  * @description:
  * @since 1.0
  */
object TagsAPP extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    val appdoc: Broadcast[collection.Map[String, String]] = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]

    // 获取appname和appid
    val appname: String = row.getAs[String]("appname")
    val appid: String = row.getAs[String]("appid")

    if (StringUtils.isNotBlank(appname)) {
      list :+= ("APP" + appname, 1)
    } else {
      list :+= ("APP" + appdoc.value.getOrElse(appid, "其他"), 1)
    }

    list
  }


}
