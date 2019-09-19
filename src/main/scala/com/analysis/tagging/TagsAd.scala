package com.analysis.tagging

import com.utils.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  *
  * @author: ZDC
  * @date: 2019/09/19 10:20
  * @description:
  * @since 1.0
  */
object TagsAd extends Tag {

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 获取数据类型
    val row: Row = args(0).asInstanceOf[Row]

    // 获取广告位类型和名称
    val adType: Int = row.getAs[Int]("adspacetype")
    val adName: String = row.getAs[String]("adspacetypename")

    // 类型
    adType match{
      case v if v > 9 => list :+= ("LC" + v, 1)
      case v if v > 0 && v <= 9  => list :+= ("LC0" + v, 1)
    }

    // 名称
    if(StringUtils.isBlank(adName)){
      list :+= ("LN" + adName, 1)
    }

    list
  }
}
