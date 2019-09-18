package com.utils

/**
  *
  * @author: ZDC
  * @date: 2019/09/18 9:46
  * @description: 处理指标统计
  * @since 1.0
  */
object RptUtils {
  // 处理请求数
  def reqPt(requestmode: Int,
            processnode: Int): List[Double] = {

    if (requestmode == 1 && processnode == 1) {
      List[Double](1, 0, 0)
    } else if (requestmode == 1 && processnode == 2) {
      List[Double](1, 1, 0)
    } else if (requestmode == 1 && processnode == 3) {
      List[Double](1, 1, 1)
    } else {
      List[Double](0, 0, 0)
    }
  }

  // 处理点击展示数
  def clickPt(requestmode: Int,
              iseffective: Int): List[Double] = {
    if (requestmode == 2 && iseffective == 1) {
      List[Double](1, 0)
    } else if (requestmode == 3 && iseffective == 1) {
      List[Double](0, 1)
    } else {
      List[Double](0, 0)
    }

  }

  // 处理竞价 成功数 广告成本 和 消费
  def adPt(iseffective: Int,
           isbilling: Int,
           isbid: Int,
           iswin: Int,
           adorderid: Int,
           winprice: Double,
           adpayment: Double): List[Double] = {

    if (iseffective == 1 && isbilling == 1 && isbid == 1) {
      if (iswin == 1 && adorderid != 0) {
        List[Double](1, 1, winprice / 1000.0, adpayment / 1000.0)
      } else {
        List[Double](1, 0, 0, 0)
      }
    } else {
      List[Double](0, 0, 0, 0)
    }

  }
}
