package com.analysis.location

import com.utils.RptUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  *
  * @author: ZDC
  * @date: 2019/09/17 17:52
  * @description: 运营分析指标
  * @since 1.0
  */
object T_02_Run {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("输入目录不正确")
      sys.exit()
    }

    val Array(inputPath, docs) = args

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("ct")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val srcDataFrame: DataFrame = sparkSession.read.parquet(inputPath)

    // 运营
    //    runDisDSL(sparkSession, srcDataFrame)

    // 网络
    //     netDisDSL(sparkSession, srcDataFrame)

    // 设备
        deviceDisDSL(sparkSession, srcDataFrame)

    // 操作系统
        clienteDisDSL(sparkSession, srcDataFrame)


    sparkSession.stop()
  }

  def clienteDisDSL(sparkSession: SparkSession,
                    srcDataFrame: DataFrame): Unit = {
    val res = srcDataFrame.rdd.map(row => {
      val reqAllList = getReqList(row)
      val clientNum: Int = row.getAs[Int]("client")
      val clientOS: String = clientNum match {
        case 1 => "android"
        case 2 => "ios"
        case 3 => "wp"
        case _ => "其他"
      }

      (clientOS, reqAllList)
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => t._1 + "," + t._2.mkString(","))
    res.foreach(println(_))
  }

  def deviceDisDSL(sparkSession: SparkSession,
                   srcDataFrame: DataFrame): Unit = {
    val res = srcDataFrame.rdd.map(row => {
      val reqAllList = getReqList(row)
      val deviceTypeNum: Int = row.getAs[Int]("devicetype")

      val deviceType: String = deviceTypeNum match {
        case 1 => "手机"
        case 2 => "平板"
        case _ => "其他"
      }

      (deviceType, reqAllList)
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => t._1 +"," + t._2.mkString(","))
    res.foreach(println(_))
  }

  def netDisDSL(sparkSession: SparkSession,
                srcDataFrame: DataFrame): Unit = {
    val res = srcDataFrame.rdd.map(row => {
      val reqAllList = getReqList(row)
      (row.getAs[String]("networkmannername"), reqAllList)
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => t._1 + "," + t._2.mkString(","))
    res.foreach(println(_))
  }


  def runDisDSL(sparkSession: SparkSession,
                srcDataFrame: DataFrame): Unit = {
    val res = srcDataFrame.rdd.map(row => {
      val reqAllList = getReqList(row)
      (row.getAs[String]("ispname"), reqAllList)
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => t._1 + "," + t._2.mkString(","))
    res.foreach(println(_))
  }

  def getReqList(row: Row): List[Double] = {
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
    reqAllList
  }

}
