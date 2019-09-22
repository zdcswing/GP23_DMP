package com.analysis.tagging

import com.utils.TagUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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

//    srcDateFarme.createOrReplaceTempView("log")

    val userId2BusinessTagRDD: RDD[(String, List[(String, Int)])] = srcDateFarme.map {
      case item =>
        // 获取用户唯一ID
        val userId = TagUtils.getOneUserId(item)



        val listBusinessTag: List[(String, Int)] = BusinessTag.makeTags(item)
        (userId, listBusinessTag)
    }.rdd

    val userId2FilterBTagRDD: RDD[(String, List[(String, Int)])] = userId2BusinessTagRDD.filter {
      case (userId, listBusinessTag) =>
        listBusinessTag.size > 0
    }

    val userTagRDD: RDD[UserTag] = userId2FilterBTagRDD.map {
      case (userId, listBTag) =>
        var aggInfo: String = ""
        for (item <- listBTag) {
          val name = item._1
          val count = item._2
          aggInfo += name + "=" + count + "|"
        }
        if(aggInfo.endsWith("\\|")){
          aggInfo = aggInfo.substring(0, aggInfo.length - 1)
        }
        UserTag(userId, aggInfo)
    }

    userTagRDD.foreach(println(_))

    println("===========ok===========")

    sparkSession.stop()

  }
}

case class UserTag(userId:String, BTag:String )