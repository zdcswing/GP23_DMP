package com.location

import com.location.T_02_Run.getReqList
import com.utils.RptUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author: ZDC
  * @date: 2019/09/18 10:52
  * @description: 媒体分析指标
  * @since 1.0
  */
class T_03_APP {

}

object T_03_APP {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("输入目录不正确")
      sys.exit()
    }

    val Array(inputPath, docs) = args

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("app")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 读取数据字典
    val doc2FilterMap: collection.Map[String, String] = sparkSession.sparkContext.textFile(docs)
      .map(_.split("\\s", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()

    // 进行广播
    val doc2FilterMapBd: Broadcast[collection.Map[String, String]] = sparkSession.sparkContext.broadcast(doc2FilterMap)

    val df: DataFrame = sparkSession.read.parquet(inputPath)

    val res = df.rdd.map(row => {
      var appName = row.getAs[String]("appname")
      if(StringUtils.isBlank(appName)){
        appName = doc2FilterMapBd.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }
      val reqAllList = getReqList(row)
      (appName, reqAllList)
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => t._1 + "," + t._2.mkString(","))

    res.foreach(println(_))
//      .saveAsTextFile("")

  }
}








