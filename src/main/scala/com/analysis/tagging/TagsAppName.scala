package com.analysis.tagging

import com.utils.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  *
  * @author: ZDC
  * @date: 2019/09/19 11:18
  * @description:
  * @since 1.0
  */
object TagsAppName extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()

    // 获取数据类型
    val row: Row = args(0).asInstanceOf[Row]

    // 获取广告位类型和名称
    val adType: Int = row.getAs[Int]("adspacetype")
    val adName: String = row.getAs[String]("adspacetypename")

    list
  }
}
