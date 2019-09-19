package com.analysis.tagging

import com.utils.TagUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author: ZDC
  * @date: 2019/09/19 9:51
  * @description:
  * @since 1.0
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("输入目录不正确")
      sys.exit()
    }

    val Array(inputPath) = args

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("TagsContext")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import sparkSession.implicits._

    val srcDateFarme: DataFrame = sparkSession.read.parquet(inputPath)

    srcDateFarme.createOrReplaceTempView("log")

//    srcDateFarme.map{
//      case item =>
//        // 获取用户唯一ID
//        val userId = TagUtils.getOneUserId(item)
//    }

    println("a===========")

    sparkSession.stop()

  }
}
