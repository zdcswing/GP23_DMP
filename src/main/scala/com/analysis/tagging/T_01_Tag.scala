package com.analysis.tagging

import org.apache.spark.sql.SparkSession

/**
  *
  * @author: ZDC
  * @date: 2019/09/18 19:16
  * @description: 数据标签化
  * @since 1.0
  */
class T_01_Tag {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("输入目录不正确")
      sys.exit()
    }

    val Array(inputPath, docs) = args

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("app")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


  }


}
