package com.analysis.location

import com.utils.RptUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author: ZDC
  * @date: 2019/09/17 17:52
  * @description: 地域分布分析指标
  * @since 1.0
  */
object T_01_Area {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("输入目录不正确")
      sys.exit()
    }

    val Array(inputPath) = args

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("ct")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val srcDataFrame: DataFrame = sparkSession.read.parquet(inputPath)

    // areaDisSQL(sparkSession, srcDataFrame)

    areaDisDSL(sparkSession, srcDataFrame)

    sparkSession.stop()
  }

  def areaDisDSL(sparkSession: SparkSession,
                 srcDataFrame: DataFrame): Unit = {

    val res = srcDataFrame.rdd.map(row => {
      // 根据指标的字段获取数据
      //  REQUESTMODE	PROCESSNODE	ISEFFECTIVE
      // ISBILLING	ISBID	ISWIN	ADORDERID
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

      ((row.getAs[String]("provincename"), row.getAs[String]("cityname")), reqAllList)
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => t._1 + "," + t._2.mkString(","))

    res.foreach(println(_))


  }


  // sql 操作
  def areaDisSQL(sparkSession: SparkSession,
                 srcDataFrame: DataFrame): Unit = {

    srcDataFrame.createOrReplaceTempView("log")
    srcDataFrame.show(10)

    val sql =
      """
        |select
        |provincename,
        |cityname,
        |sum(case when requestmode = 1 and processnode >= 1 then 1 end) cnt_req_original,
        |sum(case when requestmode = 1 and processnode >= 2 then 1 end) cnt_req_valid,
        |sum(case when requestmode = 1 and processnode = 3 then 1 end) cnt_req_ad,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 end) cnt_rtb,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 end) cnt_success,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 end) /
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 end) * 100 rate_rtb_success,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 end) cnt_show,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 end) cnt_click,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 end) / sum(case when requestmode = 2 and iseffective = 1 then 1 end) * 100 rate_click,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice / 1000 end) dsp_cnt_consume,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment / 1000 end) dsp_cnt_cost
        |from log
        |group by provincename, cityname
        |order by provincename, cityname
      """.stripMargin

    sparkSession.sql(sql).show(1000)

    // 输出
  }


}
