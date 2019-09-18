package com.location

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
class APP {

}


object APP {
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

    df.rdd.map(row => {
      var appName = row.getAs[String]("appname")
      if(StringUtils.isBlank(appName)){
        appName = doc2FilterMapBd.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }

      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")

      // 处理请求数
      val rptList: List[Double] = RptUtils.reqPt(requestmode, processnode)
      // 处理展示消息
      val clickList: List[Double] = RptUtils.clickPt(requestmode, iseffective)
      // 处理广告
      val adList: List[Double] = RptUtils.adPt(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      // 所有指标
      val reqAllList: List[Double] = rptList ++ clickList ++ adList
      (appName, reqAllList)
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => t._1 + "," + t._2.mkString(","))
//      .saveAsTextFile("")

  }
}








