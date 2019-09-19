package com.utils

/**
  *
  * @author: ZDC
  * @date: 2019/09/19 10:38
  * @description: 测试工具类
  * @since 1.0
  */
object Test {

  def main(args: Array[String]): Unit = {

    val url = """https://restapi.amap.com/v3/geocode/regeo?output=json&location=116.310003,39.991957&key=6aff8663d91c8383a09f1a1d860529b2&radius=1000&extensions=all"""
    val responJson: String = HTTPUtil.get(url)

    println(responJson)
  }
}
