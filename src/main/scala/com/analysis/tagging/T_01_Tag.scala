package com.analysis.tagging

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author: ZDC
  * @date: 2019/09/18 19:16
  * @description: 数据标签化
  * @since 1.0
  */
object T_01_Tag {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("输入目录不正确")
      sys.exit()
    }

    val Array(inputPath) = args

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("app")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val srcDateFarme: DataFrame = sparkSession.read.parquet(inputPath)

    srcDateFarme.foreach(println(_))

  }


}
