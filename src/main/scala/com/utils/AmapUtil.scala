package com.utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

/**
  *
  * @author: ZDC
  * @date: 2019/09/19 10:47
  * @description: 从高德地图获取商圈信息
  * @since 1.0
  */
object AmapUtil {
  def getBusinessFromAmap(long: Double, lat: Double): String = {
    val location = long + "," + lat
    val url = "https://restapi.amap.com/v3/geocode/regeo?output=json&location=" + location + "&key=6aff8663d91c8383a09f1a1d860529b2&radius=1000&extensions=all"

    val jsonStr: String = HTTPUtil.get(url)

    // 解析jsonStr
    val jSONObject1: JSONObject = JSON.parseObject(jsonStr)

    val starus: Int = jSONObject1.getIntValue("status")

    if (starus == 0) return ""

    val jSONObject6: JSONObject = jSONObject1.getJSONObject("regeocode")
    if (jSONObject6 == null) return ""

    // 如果不为空 businessAreas
    val jSONObject2: JSONObject = jSONObject6.getJSONObject("addressComponent")
    if (jSONObject2 == null) return ""

    val jSONArray: JSONArray = jSONObject2.getJSONArray("businessAreas")

    if (jSONArray == null) return ""

    val result = collection.mutable.ListBuffer[String]()

    for (item <- jSONArray.toArray()) {
      if (item.isInstanceOf[JSONObject]) {
        val json = item.asInstanceOf[JSONObject]
        val name = json.getString("name")
        result.append(name)
      }
    }
    result.mkString(",")
  }

}
