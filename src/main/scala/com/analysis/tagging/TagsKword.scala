package com.analysis.tagging

import com.utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  *
  * @author: ZDC
  * @date: 2019/09/22 15:48
  * @description:
  * @since 1.0
  */
object TagsKword extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row: Row = args(0).asInstanceOf[Row]
    val stopWrods: Broadcast[collection.Map[String, Int]] = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]

    row.getAs[String]("keywords").split("\\|")
      .filter(word => word.length >= 3 && word.length <= 8 && !stopWrods.value.contains(word))
      .foreach(word => list :+= ("K" + word, 1))

    list
  }
}
